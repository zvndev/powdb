use crate::disk::DiskManager;
use crate::page::{Page, PageType, PAGE_SIZE, iter_page_slots};
use crate::types::RowId;
use std::io;
use std::path::Path;

/// A single dirty page pinned in memory for write-back coalescing.
///
/// Mission C Phase 1: the previous write path did `read_page + write_page`
/// for every insert/update/delete — two syscalls per row on `insert_batch_1k`
/// and two syscalls per row on `update_by_filter`. Keeping the last-touched
/// page live in memory collapses that to ~one read + one write per PAGE
/// instead of per ROW. For a 1000-row batch into ~40 pages, that's 2000
/// syscalls → 80 syscalls — 25x fewer trips to the OS.
struct HotPage {
    page_id: u32,
    page: Page,
    /// Set to true whenever `page` is mutated. The flush is skipped when
    /// `dirty == false` (e.g., we only read a page and never wrote).
    dirty: bool,
}

/// Manages a collection of data pages for storing rows.
/// Tracks which pages have free space for fast insertion.
pub struct HeapFile {
    disk: DiskManager,
    /// Pages with known free space.
    pages_with_space: Vec<u32>,
    /// Optional mmap for zero-syscall reads. Activated by `enable_mmap()`.
    mmap_ptr: Option<(*const u8, usize)>,
    /// Mission C Phase 1: write-back cache for the most recently touched
    /// page. All insert/update/delete operations land here first and only
    /// hit disk when a different page is accessed, a scan runs, or the
    /// heap is dropped. Invariant: at most one dirty page lives in memory.
    hot_page: Option<HotPage>,
}

impl HeapFile {
    pub fn create(path: &Path) -> io::Result<Self> {
        let disk = DiskManager::create(path)?;
        Ok(HeapFile {
            disk,
            pages_with_space: Vec::new(),
            mmap_ptr: None,
            hot_page: None,
        })
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        let disk = DiskManager::open(path)?;
        let mut pages_with_space = Vec::new();
        for i in 0..disk.num_pages() {
            if let Ok(buf) = disk.read_page(i) {
                if let Some(page) = Page::from_bytes(&buf) {
                    if page.free_space() > 64 {
                        pages_with_space.push(i);
                    }
                }
            }
        }
        Ok(HeapFile {
            disk,
            pages_with_space,
            mmap_ptr: None,
            hot_page: None,
        })
    }

    /// Flush the pinned hot page to disk if it's dirty, then drop it. A
    /// scan or a cross-page access must call this first so readers see a
    /// consistent view. Called implicitly by `enable_mmap`, `for_each_row`,
    /// `try_for_each_row`, and `Drop`.
    pub fn flush_hot_page(&mut self) -> io::Result<()> {
        if let Some(hot) = self.hot_page.take() {
            if hot.dirty {
                self.disk.write_page(hot.page_id, hot.page.as_bytes())?;
            }
        }
        Ok(())
    }

    /// Make `page_id` the hot page. If a different page is currently hot,
    /// flush it first. If the target is already hot, this is a no-op.
    fn ensure_hot(&mut self, page_id: u32) -> io::Result<()> {
        if let Some(hot) = &self.hot_page {
            if hot.page_id == page_id {
                return Ok(());
            }
        }
        self.flush_hot_page()?;
        let buf = self.disk.read_page(page_id)?;
        let page = Page::from_bytes(&buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt page"))?;
        self.hot_page = Some(HotPage { page_id, page, dirty: false });
        Ok(())
    }

    /// Install a freshly-allocated page (no disk read) as the hot page.
    /// The previous hot page, if any, is flushed first.
    fn install_fresh_hot(&mut self, page_id: u32, page: Page) -> io::Result<()> {
        self.flush_hot_page()?;
        self.hot_page = Some(HotPage { page_id, page, dirty: true });
        Ok(())
    }

    /// Activate mmap for zero-syscall reads. Call after all inserts are done.
    /// The mmap covers the current file size; new inserts will invalidate it.
    ///
    /// Mission C Phase 1: the hot page must be flushed before mmapping so
    /// the mapping sees the last dirty page's contents.
    pub fn enable_mmap(&mut self) {
        if self.mmap_ptr.is_some() {
            return;
        }
        // Flush any dirty hot page so the mmap sees the same bytes the
        // reader would expect. Silently swallow errors — we never want
        // `enable_mmap` to fail the bench harness, and the fall-back is
        // per-call mmap which is still correct (if slower).
        let _ = self.flush_hot_page();

        let num_pages = self.disk.num_pages();
        if num_pages == 0 {
            return;
        }
        let file_len = num_pages as usize * PAGE_SIZE;
        use std::os::unix::io::AsRawFd;
        let fd = self.disk.file_ref().as_raw_fd();
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                file_len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd,
                0,
            )
        };
        if ptr != libc::MAP_FAILED {
            self.mmap_ptr = Some((ptr as *const u8, file_len));
        }
    }

    /// Insert encoded row data. Returns RowId.
    ///
    /// Mission C Phase 1: uses the hot-page write-back cache. The common
    /// case — repeated inserts into the currently-hot page — does zero
    /// disk syscalls; the page stays pinned until a different page is
    /// touched or an explicit flush runs.
    pub fn insert(&mut self, row_data: &[u8]) -> io::Result<RowId> {
        // Hot-path: the pinned page already has room. This is the bench's
        // insert_batch_1k / insert_single loop.
        if let Some(hot) = self.hot_page.as_mut() {
            if let Some(slot) = hot.page.insert(row_data) {
                hot.dirty = true;
                let page_id = hot.page_id;
                let became_full = hot.page.free_space() < 64;
                if became_full {
                    if let Some(pos) = self.pages_with_space.iter().position(|p| *p == page_id) {
                        self.pages_with_space.swap_remove(pos);
                    }
                }
                return Ok(RowId { page_id, slot_index: slot });
            }
            // Hot page is full — fall through to pages_with_space. The
            // flush will happen inside `ensure_hot` when we load a
            // different page.
        }

        // Try existing pages with space.
        for idx in 0..self.pages_with_space.len() {
            let page_id = self.pages_with_space[idx];
            self.ensure_hot(page_id)?;
            let hot = self.hot_page.as_mut().unwrap();
            if let Some(slot) = hot.page.insert(row_data) {
                hot.dirty = true;
                if hot.page.free_space() < 64 {
                    self.pages_with_space.swap_remove(idx);
                }
                return Ok(RowId { page_id, slot_index: slot });
            }
            // Page doesn't fit this row; try the next one on the list.
        }

        // Allocate a new page.
        let page_id = self.disk.allocate_page()?;
        let mut page = Page::new(page_id, PageType::Data);
        let slot = page.insert(row_data)
            .expect("row too large for empty page");
        if page.free_space() >= 64 {
            self.pages_with_space.push(page_id);
        }
        self.install_fresh_hot(page_id, page)?;
        Ok(RowId { page_id, slot_index: slot })
    }

    /// Read row data by RowId.
    ///
    /// Mission F: `#[inline]` so the mmap-fast-path branch can fold into
    /// `Catalog::get → Table::get → HeapFile::get` callsites. The hot path
    /// is the mmap branch — inlining lets LTO collapse the whole chain.
    ///
    /// Mission C Phase 1: if the hot page holds `rid.page_id`, read from
    /// it directly. This is what keeps `update_by_pk` fast: the read for
    /// the old row lands on the hot page we're about to write back.
    #[inline]
    pub fn get(&self, rid: RowId) -> Option<Vec<u8>> {
        // Mission C: dirty hot page takes precedence over both mmap and
        // disk — it holds writes that haven't landed yet.
        if let Some(hot) = &self.hot_page {
            if hot.page_id == rid.page_id {
                return hot.page.get(rid.slot_index).map(|d| d.to_vec());
            }
        }

        // Fast path: mmap — read directly from mapped memory
        if let Some((ptr, len)) = self.mmap_ptr {
            let offset = rid.page_id as usize * PAGE_SIZE;
            if offset + PAGE_SIZE <= len {
                let page_bytes = unsafe {
                    std::slice::from_raw_parts(ptr.add(offset), PAGE_SIZE)
                };
                let entry_off = PAGE_SIZE - 2 - ((rid.slot_index as usize + 1) * 4);
                let slot_offset = u16::from_le_bytes(
                    page_bytes[entry_off..entry_off + 2].try_into().unwrap(),
                );
                let slot_length = u16::from_le_bytes(
                    page_bytes[entry_off + 2..entry_off + 4].try_into().unwrap(),
                );
                if slot_length == 0xFFFF {
                    return None; // deleted
                }
                let start = slot_offset as usize;
                let end = start + slot_length as usize;
                return Some(page_bytes[start..end].to_vec());
            }
        }

        let buf = self.disk.read_page(rid.page_id).ok()?;
        let page = Page::from_bytes(&buf)?;
        page.get(rid.slot_index).map(|d| d.to_vec())
    }

    /// Delete a row by marking its slot as deleted.
    ///
    /// Mission C Phase 1: land the change on the hot page so back-to-back
    /// deletes targeting the same page coalesce into one disk write.
    pub fn delete(&mut self, rid: RowId) -> io::Result<()> {
        self.ensure_hot(rid.page_id)?;
        let hot = self.hot_page.as_mut().unwrap();
        hot.page.delete(rid.slot_index);
        hot.dirty = true;
        if !self.pages_with_space.contains(&rid.page_id) {
            self.pages_with_space.push(rid.page_id);
        }
        Ok(())
    }

    /// Apply an in-place mutation to a row's raw bytes. The closure
    /// receives a `&mut [u8]` of exactly the current row size and MUST NOT
    /// change the slice length. Returns `Ok(true)` if the mutation was
    /// applied, `Ok(false)` if the row is deleted or gone.
    ///
    /// Mission C Phase 4: lets the executor's update-by-pk fast path
    /// patch a fixed-width column (e.g. `age := 42`) directly on the hot
    /// page without allocating a `Vec<Value>`, calling `decode_row`, or
    /// re-running `encode_row_into`. For a 6-column User row that was
    /// ~700ns of work per update; this primitive replaces it with a
    /// single in-memory copy plus a branch.
    #[inline]
    pub fn with_row_bytes_mut<F>(&mut self, rid: RowId, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut [u8]),
    {
        self.ensure_hot(rid.page_id)?;
        let hot = self.hot_page.as_mut().unwrap();
        if let Some(bytes) = hot.page.slot_bytes_mut(rid.slot_index) {
            f(bytes);
            hot.dirty = true;
            return Ok(true);
        }
        Ok(false)
    }

    /// Update a row. Returns new RowId (may change if row moves).
    ///
    /// Mission C Phase 1: in-place updates land on the hot page directly.
    /// `update_by_filter` and `update_by_pk` both route here.
    pub fn update(&mut self, rid: RowId, row_data: &[u8]) -> io::Result<RowId> {
        self.ensure_hot(rid.page_id)?;
        {
            let hot = self.hot_page.as_mut().unwrap();
            if hot.page.update(rid.slot_index, row_data) {
                hot.dirty = true;
                return Ok(rid);
            }
        }
        // Doesn't fit in place — delete old, insert new. Both helpers also
        // go through the hot page, so the follow-up insert typically
        // lands on the same page and avoids another read.
        self.delete(rid)?;
        self.insert(row_data)
    }

    /// Scan all live rows across all pages.
    ///
    /// Mission C Phase 1: observes the pinned hot page so callers see
    /// unflushed writes. The iterator materialises the result list up front
    /// (same as before — the returned type was already an owned flat_map),
    /// so copying the hot page bytes into the result costs nothing extra.
    pub fn scan(&self) -> impl Iterator<Item = (RowId, Vec<u8>)> + '_ {
        let hot_view = self.hot_page.as_ref().map(|hot| (hot.page_id, *hot.page.as_bytes()));
        (0..self.disk.num_pages()).flat_map(move |page_id| {
            let entries: Vec<_> = match &hot_view {
                Some((hid, hbytes)) if *hid == page_id => {
                    iter_page_slots(hbytes.as_slice())
                        .map(|(slot, data)| (RowId { page_id, slot_index: slot }, data.to_vec()))
                        .collect()
                }
                _ => self.disk.read_page(page_id).ok()
                    .and_then(|buf| Page::from_bytes(&buf))
                    .map(|page| {
                        page.iter().map(|(slot, data)| {
                            (RowId { page_id, slot_index: slot }, data.to_vec())
                        }).collect()
                    })
                    .unwrap_or_default(),
            };
            entries.into_iter()
        })
    }

    /// Zero-copy scan with early termination. The callback returns
    /// `ControlFlow::Break(())` to stop iteration immediately.
    ///
    /// Mission D2: this is the load-bearing fix for `Project(Limit(...))`
    /// fast paths. Without it, `limit 100` on a 100K-row table still walked
    /// all 100K slots — the existing `done` flag in executor only short-
    /// circuited the *body* of the closure, not the iteration itself, so
    /// the inner loop kept paying decode_column / pred / call-frame cost
    /// for the trailing 99,900 rows.
    ///
    /// Mission D6: prefer the persistent mmap set by `enable_mmap()` instead
    /// of doing mmap+munmap on every call. The bench's per-query mmap pair
    /// was a syscall pair we paid on every read query.
    #[inline]
    pub fn try_for_each_row<F>(&self, mut f: F)
    where
        F: FnMut(RowId, &[u8]) -> std::ops::ControlFlow<()>,
    {
        use std::ops::ControlFlow;

        let num_pages = self.disk.num_pages();
        if num_pages == 0 {
            return;
        }

        // Mission C Phase 1: if a hot page is pinned in memory, the scan
        // must observe its dirty contents — the mmap and the file both
        // still hold the stale version. We substitute the in-memory page
        // when the loop reaches its page_id.
        let hot_view: Option<(u32, &[u8; PAGE_SIZE])> = self
            .hot_page
            .as_ref()
            .map(|hot| (hot.page_id, hot.page.as_bytes()));

        // Fast path: persistent mmap activated by `enable_mmap()`. Zero
        // syscalls per query — we just slice the existing mapping.
        if let Some((ptr, len)) = self.mmap_ptr {
            let mapped = unsafe { std::slice::from_raw_parts(ptr, len) };
            let pages_in_map = len / PAGE_SIZE;
            let limit = num_pages.min(pages_in_map as u32);
            'outer: for page_id in 0..limit {
                let page_bytes: &[u8] = match hot_view {
                    Some((hid, hbytes)) if hid == page_id => hbytes.as_slice(),
                    _ => {
                        let offset = page_id as usize * PAGE_SIZE;
                        &mapped[offset..offset + PAGE_SIZE]
                    }
                };
                for (slot, data) in iter_page_slots(page_bytes) {
                    if let ControlFlow::Break(()) = f(RowId { page_id, slot_index: slot }, data) {
                        break 'outer;
                    }
                }
            }
            // The mmap may not have grown to cover pages allocated after
            // enable_mmap. If the hot page lives beyond that window, visit
            // it explicitly so inserts into fresh pages stay observable.
            if let Some((hid, hbytes)) = hot_view {
                if hid >= limit && hid < num_pages {
                    for (slot, data) in iter_page_slots(hbytes) {
                        if let ControlFlow::Break(()) = f(RowId { page_id: hid, slot_index: slot }, data) {
                            return;
                        }
                    }
                }
            }
            return;
        }

        // No persistent mmap — try a per-call mmap as a one-shot best effort.
        use std::os::unix::io::AsRawFd;
        let fd = self.disk.file_ref().as_raw_fd();
        let file_len = (num_pages as usize) * PAGE_SIZE;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                file_len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd,
                0,
            )
        };

        if ptr != libc::MAP_FAILED {
            let mapped = unsafe { std::slice::from_raw_parts(ptr as *const u8, file_len) };
            'outer: for page_id in 0..num_pages {
                let page_bytes: &[u8] = match hot_view {
                    Some((hid, hbytes)) if hid == page_id => hbytes.as_slice(),
                    _ => {
                        let offset = page_id as usize * PAGE_SIZE;
                        &mapped[offset..offset + PAGE_SIZE]
                    }
                };
                for (slot, data) in iter_page_slots(page_bytes) {
                    if let ControlFlow::Break(()) = f(RowId { page_id, slot_index: slot }, data) {
                        break 'outer;
                    }
                }
            }
            unsafe { libc::munmap(ptr, file_len); }
        } else {
            // Fallback: per-page read.
            'outer: for page_id in 0..num_pages {
                if let Some((hid, hbytes)) = hot_view {
                    if hid == page_id {
                        for (slot, data) in iter_page_slots(hbytes) {
                            if let ControlFlow::Break(()) = f(RowId { page_id, slot_index: slot }, data) {
                                break 'outer;
                            }
                        }
                        continue;
                    }
                }
                let buf = match self.disk.read_page(page_id) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                if let Some(page) = Page::from_bytes(&buf) {
                    for (slot, data) in page.iter() {
                        if let ControlFlow::Break(()) = f(RowId { page_id, slot_index: slot }, data) {
                            break 'outer;
                        }
                    }
                }
            }
        }
    }

    /// Zero-copy scan: calls `f` for every live row without allocating a
    /// `Vec<u8>` per row. Uses the persistent mmap activated by
    /// `enable_mmap()` when available, otherwise falls back to a per-call
    /// mmap or page-by-page read.
    ///
    /// Mission D6: same persistent-mmap fix as `try_for_each_row`.
    ///
    /// Mission C Phase 1: same hot-page substitution as `try_for_each_row`.
    /// Scans of tables with unflushed writes see the latest bytes via the
    /// in-memory page rather than the stale disk page.
    #[inline]
    pub fn for_each_row<F>(&self, mut f: F)
    where
        F: FnMut(RowId, &[u8]),
    {
        let num_pages = self.disk.num_pages();
        if num_pages == 0 {
            return;
        }

        let hot_view: Option<(u32, &[u8; PAGE_SIZE])> = self
            .hot_page
            .as_ref()
            .map(|hot| (hot.page_id, hot.page.as_bytes()));

        // Fast path: persistent mmap.
        if let Some((ptr, len)) = self.mmap_ptr {
            let mapped = unsafe { std::slice::from_raw_parts(ptr, len) };
            let pages_in_map = len / PAGE_SIZE;
            let limit = num_pages.min(pages_in_map as u32);
            for page_id in 0..limit {
                let page_bytes: &[u8] = match hot_view {
                    Some((hid, hbytes)) if hid == page_id => hbytes.as_slice(),
                    _ => {
                        let offset = page_id as usize * PAGE_SIZE;
                        &mapped[offset..offset + PAGE_SIZE]
                    }
                };
                for (slot, data) in iter_page_slots(page_bytes) {
                    f(RowId { page_id, slot_index: slot }, data);
                }
            }
            // Hot page allocated after enable_mmap — visit it explicitly.
            if let Some((hid, hbytes)) = hot_view {
                if hid >= limit && hid < num_pages {
                    for (slot, data) in iter_page_slots(hbytes) {
                        f(RowId { page_id: hid, slot_index: slot }, data);
                    }
                }
            }
            return;
        }

        // No persistent mmap — try a per-call mmap as a one-shot best effort.
        use std::os::unix::io::AsRawFd;
        let fd = self.disk.file_ref().as_raw_fd();
        let file_len = (num_pages as usize) * PAGE_SIZE;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                file_len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd,
                0,
            )
        };

        if ptr != libc::MAP_FAILED {
            let mapped = unsafe { std::slice::from_raw_parts(ptr as *const u8, file_len) };
            for page_id in 0..num_pages {
                let page_bytes: &[u8] = match hot_view {
                    Some((hid, hbytes)) if hid == page_id => hbytes.as_slice(),
                    _ => {
                        let offset = page_id as usize * PAGE_SIZE;
                        &mapped[offset..offset + PAGE_SIZE]
                    }
                };
                for (slot, data) in iter_page_slots(page_bytes) {
                    f(RowId { page_id, slot_index: slot }, data);
                }
            }
            unsafe { libc::munmap(ptr, file_len); }
        } else {
            // Fallback: per-page read
            for page_id in 0..num_pages {
                if let Some((hid, hbytes)) = hot_view {
                    if hid == page_id {
                        for (slot, data) in iter_page_slots(hbytes) {
                            f(RowId { page_id, slot_index: slot }, data);
                        }
                        continue;
                    }
                }
                let buf = match self.disk.read_page(page_id) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                if let Some(page) = Page::from_bytes(&buf) {
                    for (slot, data) in page.iter() {
                        f(RowId { page_id, slot_index: slot }, data);
                    }
                }
            }
        }
    }

    /// Mission C Phase 1: flush the hot page (if dirty) before syncing the
    /// underlying file. A bare `disk.flush()` would otherwise miss the
    /// in-memory dirty buffer.
    pub fn flush(&mut self) -> io::Result<()> {
        self.flush_hot_page()?;
        self.disk.flush()
    }
}

impl Drop for HeapFile {
    fn drop(&mut self) {
        // Mission C Phase 1: persist the hot page before the file handle
        // goes away. Without this, the final write-back of a bench's last
        // batch would be lost on close.
        let _ = self.flush_hot_page();
        if let Some((ptr, len)) = self.mmap_ptr.take() {
            unsafe { libc::munmap(ptr as *mut libc::c_void, len); }
        }
    }
}

// SAFETY: The mmap pointer is read-only and the file is not modified
// while the map is active. The HeapFile is not Send/Sync anyway (it
// contains DiskManager with File), so this is fine for single-threaded use.
unsafe impl Send for HeapFile {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use crate::row::{encode_row, decode_row};

    fn user_schema() -> Schema {
        Schema {
            table_name: "users".into(),
            columns: vec![
                ColumnDef { name: "name".into(),  type_id: TypeId::Str, required: true,  position: 0 },
                ColumnDef { name: "age".into(),    type_id: TypeId::Int, required: false, position: 1 },
            ],
        }
    }

    fn temp_heap(name: &str) -> (HeapFile, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("powdb_heap_{name}_{}", std::process::id()));
        let heap = HeapFile::create(&path).unwrap();
        (heap, path)
    }

    #[test]
    fn test_insert_and_get() {
        let (mut heap, path) = temp_heap("basic");
        let schema = user_schema();
        let row = vec![Value::Str("Alice".into()), Value::Int(30)];
        let encoded = encode_row(&schema, &row);
        let rid = heap.insert(&encoded).unwrap();
        let data = heap.get(rid).unwrap();
        let decoded = decode_row(&schema, &data);
        assert_eq!(decoded[0], Value::Str("Alice".into()));
        assert_eq!(decoded[1], Value::Int(30));
        drop(heap);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_scan_all_rows() {
        let (mut heap, path) = temp_heap("scan");
        let schema = user_schema();
        for i in 0..100 {
            let row = vec![Value::Str(format!("user_{i}")), Value::Int(i)];
            heap.insert(&encode_row(&schema, &row)).unwrap();
        }
        let all: Vec<_> = heap.scan().collect();
        assert_eq!(all.len(), 100);
        drop(heap);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_delete_row() {
        let (mut heap, path) = temp_heap("del");
        let schema = user_schema();
        let r1 = heap.insert(&encode_row(&schema, &vec![Value::Str("A".into()), Value::Int(1)])).unwrap();
        let r2 = heap.insert(&encode_row(&schema, &vec![Value::Str("B".into()), Value::Int(2)])).unwrap();
        heap.delete(r1).unwrap();
        assert!(heap.get(r1).is_none());
        assert!(heap.get(r2).is_some());
        assert_eq!(heap.scan().count(), 1);
        drop(heap);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_update_row() {
        let (mut heap, path) = temp_heap("upd");
        let schema = user_schema();
        let row = vec![Value::Str("Alice".into()), Value::Int(30)];
        let rid = heap.insert(&encode_row(&schema, &row)).unwrap();
        let new_row = vec![Value::Str("Alice".into()), Value::Int(31)];
        let new_rid = heap.update(rid, &encode_row(&schema, &new_row)).unwrap();
        let decoded = decode_row(&schema, &heap.get(new_rid).unwrap());
        assert_eq!(decoded[1], Value::Int(31));
        drop(heap);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_multi_page_span() {
        let (mut heap, path) = temp_heap("multipage");
        let schema = user_schema();
        // Insert enough rows to span multiple pages
        for i in 0..500 {
            let row = vec![Value::Str(format!("user_{i:04}")), Value::Int(i)];
            heap.insert(&encode_row(&schema, &row)).unwrap();
        }
        assert_eq!(heap.scan().count(), 500);
        drop(heap);
        std::fs::remove_file(&path).ok();
    }
}
