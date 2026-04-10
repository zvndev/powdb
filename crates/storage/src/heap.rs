use crate::disk::DiskManager;
use crate::page::{Page, PageType, PAGE_SIZE, iter_page_slots};
use crate::types::RowId;
use rustc_hash::FxHashMap;
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
    /// Pages with known free space. Iteration order matters for the
    /// `insert` fallback path, so this is a `Vec`. Membership is tracked
    /// in `in_free_list` for O(1) `contains` checks.
    pages_with_space: Vec<u32>,
    /// Mission C Phase 8: sidecar bitmap parallel to `pages_with_space`
    /// that answers "is page N already on the free-space list?" in O(1).
    /// Indexed by page_id. Previously `delete` did a linear `contains`
    /// over `pages_with_space` on every call — for a scattered delete
    /// that walks every page, that's quadratic in the number of pages
    /// with free space and shows up as a ~30% overhead on
    /// `delete_by_filter`.
    in_free_list: Vec<bool>,
    /// Optional mmap for zero-syscall reads. Activated by `enable_mmap()`.
    mmap_ptr: Option<(*const u8, usize)>,
    /// Mission C Phase 1: write-back cache for the most recently touched
    /// page. All insert/update/delete operations land here first and only
    /// hit disk when a different page is accessed, a scan runs, or the
    /// heap is dropped. Invariant: at most one dirty page lives in memory.
    hot_page: Option<HotPage>,
    /// Mission C Phase 9: deferred-write buffer for pages that were
    /// previously hot, got mutated, and have since been evicted from the
    /// `hot_page` slot. The previous design synchronously wrote the old
    /// hot page to disk on every page transition — for a scattered
    /// `delete_by_filter` that walks ~2000 pages, that's ~2000
    /// `write_page` syscalls on the critical path. Now the evicted dirty
    /// page gets parked here in memory, reclaimed on the next access to
    /// the same page, and only persisted via an explicit
    /// [`flush_all_dirty`] call (or `Drop`). Scan operations call
    /// `flush_all_dirty` first so their view is consistent with disk.
    dirty_buffer: FxHashMap<u32, Page>,
}

impl HeapFile {
    pub fn create(path: &Path) -> io::Result<Self> {
        let disk = DiskManager::create(path)?;
        Ok(HeapFile {
            disk,
            pages_with_space: Vec::new(),
            in_free_list: Vec::new(),
            mmap_ptr: None,
            hot_page: None,
            dirty_buffer: FxHashMap::default(),
        })
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        let mut disk = DiskManager::open(path)?;
        let num_pages = disk.num_pages();
        let mut pages_with_space = Vec::new();
        let mut in_free_list = vec![false; num_pages as usize];
        for i in 0..num_pages {
            if let Ok(buf) = disk.read_page(i) {
                // Mission 2: a page whose `page_type` byte is 0 was
                // allocated by [`DiskManager::allocate_page`] (which
                // writes an all-zero 4KB block) but never populated with
                // a real [`Page`] header. This happens when a crash
                // occurs between `allocate_page` extending the file and
                // the first `write_page` that lands actual row data —
                // which is exactly the state a WAL-replay-driven recovery
                // sees. Reinitialize these pages in place as fresh empty
                // Data pages so the insert path can treat them as normal
                // free-space candidates. Without this, `Page::insert`
                // takes `free_start = 0` as the write offset and stomps
                // on the page header with row bytes.
                if buf[4] == 0 {
                    let fresh = Page::new(i, PageType::Data);
                    let _ = disk.write_page(i, fresh.as_bytes());
                    pages_with_space.push(i);
                    in_free_list[i as usize] = true;
                    continue;
                }
                if let Some(page) = Page::from_bytes(&buf) {
                    if page.free_space() > 64 {
                        pages_with_space.push(i);
                        in_free_list[i as usize] = true;
                    }
                }
            }
        }
        Ok(HeapFile {
            disk,
            pages_with_space,
            in_free_list,
            mmap_ptr: None,
            hot_page: None,
            dirty_buffer: FxHashMap::default(),
        })
    }

    /// O(1) check: is `page_id` currently on the free-space list?
    #[inline]
    fn is_in_free_list(&self, page_id: u32) -> bool {
        self.in_free_list
            .get(page_id as usize)
            .copied()
            .unwrap_or(false)
    }

    /// Mark `page_id` as no-longer-free in the sidecar bitmap. Caller is
    /// responsible for removing it from `pages_with_space`.
    #[inline]
    fn mark_not_free(&mut self, page_id: u32) {
        if let Some(slot) = self.in_free_list.get_mut(page_id as usize) {
            *slot = false;
        }
    }

    /// Mark `page_id` as free in the sidecar bitmap, growing the vec if
    /// the id is beyond current capacity. Caller is responsible for
    /// pushing it onto `pages_with_space`.
    #[inline]
    fn mark_free(&mut self, page_id: u32) {
        let idx = page_id as usize;
        if idx >= self.in_free_list.len() {
            self.in_free_list.resize(idx + 1, false);
        }
        self.in_free_list[idx] = true;
    }

    /// Park the pinned hot page into the deferred-write buffer (or drop
    /// it if clean). Does not write to disk. Use [`flush_all_dirty`]
    /// to persist every buffered page.
    ///
    /// Mission C Phase 9: this was previously a write-through. Now the
    /// only callers that actually touch disk are `flush_all_dirty`,
    /// `enable_mmap`, and `Drop`.
    fn park_hot_page(&mut self) {
        if let Some(hot) = self.hot_page.take() {
            if hot.dirty {
                self.dirty_buffer.insert(hot.page_id, hot.page);
            }
        }
    }

    /// Public API kept for compatibility: park the hot page AND flush
    /// every buffered dirty page to disk. Callers that need an on-disk
    /// consistent view (scans, mmap rebuilds) use this.
    pub fn flush_hot_page(&mut self) -> io::Result<()> {
        self.flush_all_dirty()
    }

    /// Write every buffered dirty page to disk, including the current
    /// hot page if dirty. Clears the buffer. Called by scans,
    /// `enable_mmap`, explicit flush requests, and `Drop`.
    pub fn flush_all_dirty(&mut self) -> io::Result<()> {
        if let Some(hot) = self.hot_page.as_mut() {
            if hot.dirty {
                self.disk.write_page(hot.page_id, hot.page.as_bytes())?;
                hot.dirty = false;
            }
        }
        if !self.dirty_buffer.is_empty() {
            // Drain via a swap to avoid borrowing `self` twice.
            let drained: Vec<(u32, Page)> = self.dirty_buffer.drain().collect();
            for (page_id, page) in drained {
                self.disk.write_page(page_id, page.as_bytes())?;
            }
        }
        Ok(())
    }

    /// Make `page_id` the hot page. If a different page is currently hot,
    /// park it into the deferred-write buffer first. If `page_id` is
    /// already in the buffer, reclaim it instead of re-reading from disk.
    ///
    /// Mission C Phase 12: if `mmap_ptr` is set and covers the page, copy
    /// the bytes directly from the mapped region instead of issuing a
    /// `pread` syscall. On `delete_by_filter` this removes ~1.7ms of
    /// scattered syscall overhead (3000+ pages × ~500ns each).
    fn ensure_hot(&mut self, page_id: u32) -> io::Result<()> {
        if let Some(hot) = &self.hot_page {
            if hot.page_id == page_id {
                return Ok(());
            }
        }
        self.park_hot_page();

        // Mission C Phase 9: reclaim the page from the dirty buffer if
        // we've touched it before. This is the hot path for scattered
        // delete/update workloads — we re-visit the same pages via the
        // index lookups and don't want to re-read them from disk.
        if let Some(page) = self.dirty_buffer.remove(&page_id) {
            self.hot_page = Some(HotPage { page_id, page, dirty: true });
            return Ok(());
        }

        // Mission C Phase 12: zero-syscall read via mmap. The mmap was
        // created from a consistent on-disk snapshot after populate; as
        // long as the page hasn't been mutated since (i.e., not in hot/
        // dirty_buffer — already checked above), the bytes we see are
        // what `disk.read_page` would return without the syscall.
        if let Some((ptr, len)) = self.mmap_ptr {
            let offset = page_id as usize * PAGE_SIZE;
            if offset + PAGE_SIZE <= len {
                let page_bytes = unsafe {
                    std::slice::from_raw_parts(ptr.add(offset), PAGE_SIZE)
                };
                if let Some(page) = Page::from_bytes(page_bytes) {
                    self.hot_page = Some(HotPage { page_id, page, dirty: false });
                    return Ok(());
                }
            }
        }

        let buf = self.disk.read_page(page_id)?;
        let page = Page::from_bytes(&buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt page"))?;
        self.hot_page = Some(HotPage { page_id, page, dirty: false });
        Ok(())
    }

    /// Install a freshly-allocated page (no disk read) as the hot page.
    /// The previous hot page, if any, is parked into the dirty buffer.
    fn install_fresh_hot(&mut self, page_id: u32, page: Page) -> io::Result<()> {
        self.park_hot_page();
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
        // Flush every dirty page (hot + buffered) so the mmap sees the
        // same bytes the reader would expect. Silently swallow errors —
        // we never want `enable_mmap` to fail the bench harness, and the
        // fall-back is per-call mmap which is still correct (if slower).
        let _ = self.flush_all_dirty();

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
                    self.mark_not_free(page_id);
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
                    self.mark_not_free(page_id);
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
            self.mark_free(page_id);
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

        // Mission C Phase 9: parked dirty page is also authoritative
        // over mmap/disk.
        if let Some(page) = self.dirty_buffer.get(&rid.page_id) {
            return page.get(rid.slot_index).map(|d| d.to_vec());
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
        // Mission C Phase 8: O(1) membership check via sidecar bitmap.
        // On scattered `delete_by_filter` runs this used to be the single
        // biggest hot-loop cost — `Vec::contains` grows linearly as pages
        // get added to the free list.
        if !self.is_in_free_list(rid.page_id) {
            self.pages_with_space.push(rid.page_id);
            self.mark_free(rid.page_id);
        }
        Ok(())
    }

    /// Delete a row while giving the caller access to the old bytes in a
    /// single `ensure_hot` pass. The closure runs against the live row
    /// bytes *before* the slot is marked deleted — callers use this to
    /// pull out index keys for secondary-index maintenance without
    /// paying for a second `ensure_hot` round-trip.
    ///
    /// Mission C Phase 12: `Table::delete_many` threads the index-key
    /// extraction through this primitive, so a bulk delete does one
    /// ensure_hot per row instead of two. For a 20K-row
    /// `delete_by_filter` that saves ~800μs of redundant hot-slot lookups.
    ///
    /// Returns `Ok(true)` if the slot was found and deleted, `Ok(false)`
    /// if the slot was already missing (caller should treat as a no-op).
    #[inline]
    pub fn delete_with_hook<F>(&mut self, rid: RowId, hook: F) -> io::Result<bool>
    where
        F: FnOnce(&[u8]),
    {
        self.ensure_hot(rid.page_id)?;
        let found = {
            let hot = self.hot_page.as_mut().unwrap();
            // Run the hook under a scoped immutable borrow of the page,
            // then drop that borrow before re-borrowing mutably for
            // `delete`.
            let has_slot = if let Some(bytes) = hot.page.get(rid.slot_index) {
                hook(bytes);
                true
            } else {
                false
            };
            if has_slot {
                hot.page.delete(rid.slot_index);
                hot.dirty = true;
            }
            has_slot
        };
        if found && !self.is_in_free_list(rid.page_id) {
            self.pages_with_space.push(rid.page_id);
            self.mark_free(rid.page_id);
        }
        Ok(found)
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

    /// Apply an in-place mutation that may SHRINK a row. The closure
    /// receives `&mut [u8]` of the current row and returns `Some(new_len)`
    /// if the mutation succeeded (with `new_len <= current len`), or
    /// `None` to signal "doesn't fit in place, caller should fall back".
    /// On success the slot directory is updated so the row is now
    /// `new_len` bytes long.
    ///
    /// Mission C Phase 10: backs the var-column update fast path for
    /// `update_by_filter`. The closure uses
    /// [`crate::row::patch_var_column_in_place`] to rewrite the single
    /// changed var column in the row's raw bytes without invoking
    /// `decode_row` / `encode_row_into`.
    ///
    /// Returns `Ok(true)` if the patch landed, `Ok(false)` if the row is
    /// deleted/missing OR the closure returned `None`.
    #[inline]
    pub fn patch_row_shrink<F>(&mut self, rid: RowId, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut [u8]) -> Option<u16>,
    {
        self.ensure_hot(rid.page_id)?;
        let hot = self.hot_page.as_mut().unwrap();
        let Some(bytes) = hot.page.slot_bytes_mut(rid.slot_index) else {
            return Ok(false);
        };
        let old_len = bytes.len();
        let Some(new_len) = f(bytes) else {
            return Ok(false);
        };
        // Defence in depth: the helper's contract says new_len <= old_len,
        // but if a bug upstream lies to us we don't want to silently corrupt
        // the slot directory.
        if (new_len as usize) > old_len {
            return Ok(false);
        }
        if (new_len as usize) != old_len {
            hot.page.shrink_slot(rid.slot_index, new_len);
        }
        hot.dirty = true;
        Ok(true)
    }

    /// Apply a borrowed read to a row's raw bytes. Like
    /// [`with_row_bytes_mut`] but without the mutable-access path — the
    /// closure sees the row slice, runs, and returns. No `Vec<u8>` is
    /// allocated, so callers that only want to decode a few columns
    /// (e.g. the index-maintenance side of `Table::delete`) can skip the
    /// per-row clone that `HeapFile::get` would otherwise force.
    ///
    /// Mission C Phase 7: this is the read-side counterpart to the
    /// write-side primitive that backs the Mission C Phase 4 update fast
    /// path. Same rationale — avoid allocating a whole row buffer just
    /// to read a handful of bytes out of it.
    #[inline]
    pub fn with_row_bytes<R, F>(&mut self, rid: RowId, f: F) -> io::Result<Option<R>>
    where
        F: FnOnce(&[u8]) -> R,
    {
        self.ensure_hot(rid.page_id)?;
        let hot = self.hot_page.as_ref().unwrap();
        if let Some(bytes) = hot.page.get(rid.slot_index) {
            return Ok(Some(f(bytes)));
        }
        Ok(None)
    }

    /// Single-pass scan-and-delete. Walks every page in order, running
    /// `pred` on each live row's raw bytes. When `pred` returns `true`,
    /// `hook` is called with the same bytes (caller uses this to extract
    /// index keys before the slot is cleared) and the slot is marked
    /// deleted in place. Returns the total number of rows removed.
    ///
    /// Mission C Phase 16: fuses `collect_rids_for_mutation` +
    /// `delete_many` into one traversal. The old path did two walks over
    /// the heap — first building a `Vec<RowId>` via `for_each_row` (reads
    /// from mmap), then visiting each rid via `delete_with_hook` which
    /// called `ensure_hot(rid.page_id)` per row. Even when the rids were
    /// already sorted by page_id, every page boundary cost a
    /// `park_hot_page` + `Page::from_bytes` (4KB memcpy from the dirty
    /// buffer or mmap). For a 100K-row `delete_by_filter` with ~20K
    /// matches spread across ~3000 pages, that was ~3000 redundant page
    /// installs worth ~500-800ns each — meaningful slice of a ~1.9ms
    /// query. This primitive does exactly one `ensure_hot` per page and
    /// mutates in place under the single pinned borrow.
    #[inline]
    pub fn scan_delete_matching<P, H>(
        &mut self,
        mut pred: P,
        mut hook: H,
    ) -> io::Result<u64>
    where
        P: FnMut(&[u8]) -> bool,
        H: FnMut(RowId, &[u8]),
    {
        let num_pages = self.disk.num_pages();
        if num_pages == 0 {
            return Ok(0);
        }
        let mut count = 0u64;
        for page_id in 0..num_pages {
            self.ensure_hot(page_id)?;
            let mut any_deleted = false;
            {
                let hot = self.hot_page.as_mut().unwrap();
                let slot_count = hot.page.slot_count();
                for slot in 0..slot_count {
                    // Scoped immutable borrow for the pred/hook invocation,
                    // then a separate mutable call to `delete`. The borrow
                    // checker is happy because each borrow ends inside the
                    // same iteration.
                    let should_delete = match hot.page.get(slot) {
                        Some(bytes) => {
                            if pred(bytes) {
                                // Mission B2: hook receives the rid so the
                                // catalog's WAL-logged wrapper can emit one
                                // Delete record per matched row in the same
                                // single-pass scan.
                                hook(RowId { page_id, slot_index: slot }, bytes);
                                true
                            } else {
                                false
                            }
                        }
                        None => false,
                    };
                    if should_delete {
                        hot.page.delete(slot);
                        any_deleted = true;
                        count += 1;
                    }
                }
                if any_deleted {
                    hot.dirty = true;
                }
            }
            if any_deleted && !self.is_in_free_list(page_id) {
                self.pages_with_space.push(page_id);
                self.mark_free(page_id);
            }
        }
        Ok(count)
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
            // Mission C Phase 9: parked dirty pages override disk.
            if let Some(page) = self.dirty_buffer.get(&page_id) {
                let entries: Vec<_> = page.iter()
                    .map(|(slot, data)| (RowId { page_id, slot_index: slot }, data.to_vec()))
                    .collect();
                return entries.into_iter();
            }
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
                // Mission C Phase 9: dirty buffer > hot page > mmap.
                if let Some(page) = self.dirty_buffer.get(&page_id) {
                    for (slot, data) in iter_page_slots(page.as_bytes()) {
                        if let ControlFlow::Break(()) = f(RowId { page_id, slot_index: slot }, data) {
                            break 'outer;
                        }
                    }
                    continue;
                }
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
                if hid >= limit && hid < num_pages && !self.dirty_buffer.contains_key(&hid) {
                    for (slot, data) in iter_page_slots(hbytes) {
                        if let ControlFlow::Break(()) = f(RowId { page_id: hid, slot_index: slot }, data) {
                            return;
                        }
                    }
                }
            }
            // Visit any dirty-buffered pages that sit beyond the mmap
            // window (pages allocated after enable_mmap that we then
            // evicted from the hot slot).
            for page_id in limit..num_pages {
                if let Some(page) = self.dirty_buffer.get(&page_id) {
                    for (slot, data) in iter_page_slots(page.as_bytes()) {
                        if let ControlFlow::Break(()) = f(RowId { page_id, slot_index: slot }, data) {
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
                // Mission C Phase 9: dirty buffer has priority.
                if let Some(page) = self.dirty_buffer.get(&page_id) {
                    for (slot, data) in iter_page_slots(page.as_bytes()) {
                        if let ControlFlow::Break(()) = f(RowId { page_id, slot_index: slot }, data) {
                            break 'outer;
                        }
                    }
                    continue;
                }
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
                if let Some(page) = self.dirty_buffer.get(&page_id) {
                    for (slot, data) in iter_page_slots(page.as_bytes()) {
                        if let ControlFlow::Break(()) = f(RowId { page_id, slot_index: slot }, data) {
                            break 'outer;
                        }
                    }
                    continue;
                }
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
                if let Some(page) = self.dirty_buffer.get(&page_id) {
                    for (slot, data) in iter_page_slots(page.as_bytes()) {
                        f(RowId { page_id, slot_index: slot }, data);
                    }
                    continue;
                }
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
                if hid >= limit && hid < num_pages && !self.dirty_buffer.contains_key(&hid) {
                    for (slot, data) in iter_page_slots(hbytes) {
                        f(RowId { page_id: hid, slot_index: slot }, data);
                    }
                }
            }
            for page_id in limit..num_pages {
                if let Some(page) = self.dirty_buffer.get(&page_id) {
                    for (slot, data) in iter_page_slots(page.as_bytes()) {
                        f(RowId { page_id, slot_index: slot }, data);
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
                if let Some(page) = self.dirty_buffer.get(&page_id) {
                    for (slot, data) in iter_page_slots(page.as_bytes()) {
                        f(RowId { page_id, slot_index: slot }, data);
                    }
                    continue;
                }
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
                if let Some(page) = self.dirty_buffer.get(&page_id) {
                    for (slot, data) in iter_page_slots(page.as_bytes()) {
                        f(RowId { page_id, slot_index: slot }, data);
                    }
                    continue;
                }
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
        // Mission C Phase 1 / Phase 9: persist the hot page AND every
        // parked dirty page before the file handle goes away. Without
        // this, the final write-back of a bench's last batch (and of
        // any deferred-flush mutations) would be lost on close.
        let _ = self.flush_all_dirty();
        if let Some((ptr, len)) = self.mmap_ptr.take() {
            unsafe { libc::munmap(ptr as *mut libc::c_void, len); }
        }
    }
}

// SAFETY: The mmap pointer is read-only and the file is not modified
// while the map is active. The HeapFile is not Send/Sync anyway (it
// contains DiskManager with File), so this is fine for single-threaded use.
unsafe impl Send for HeapFile {}
// SAFETY: Blocker B1 fix. `HeapFile` lives behind `Arc<RwLock<Engine>>`,
// so the standard `&self`/`&mut self` discipline applies: many readers
// or one writer, never both. The interesting question is whether the
// `&self` read path is itself thread-safe across multiple reader threads.
//
// The disk fallback (`DiskManager::read_page` / `write_page`) now uses
// `FileExt::read_exact_at` / `write_all_at`, which map to pread(2) /
// pwrite(2). POSIX guarantees these are atomic with respect to the kernel
// file offset, so concurrent `&self` callers sharing a single `&File`
// cannot race on a seek cursor the way a `seek + read_exact` pair would.
// Byte-level corruption under concurrent reads — the old bug — is gone.
//
// The `mmap_ptr` field is a `*const u8` into a read-only mmap. Read-only
// `&[u8]` views derived via `std::slice::from_raw_parts` are fine to
// alias across threads: no `&mut` can coexist with the readers because
// the RwLock write guard excludes them. Writers still take the write
// guard for higher-level consistency (catalog/header mutation); this
// SAFETY note is strictly about the read path not corrupting bytes.
unsafe impl Sync for HeapFile {}

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
    fn test_scan_delete_matching_basic() {
        let (mut heap, path) = temp_heap("sdm_basic");
        let schema = user_schema();
        // Insert enough rows to span multiple pages so the per-page
        // ensure_hot loop is actually exercised.
        let mut inserted = Vec::new();
        for i in 0..500 {
            let row = vec![Value::Str(format!("user_{i:04}")), Value::Int(i)];
            inserted.push(heap.insert(&encode_row(&schema, &row)).unwrap());
        }

        // Delete every row whose age is even via raw-bytes predicate.
        // The age column is at schema position 1 (after the name str).
        let layout = crate::row::RowLayout::new(&schema);
        let mut deleted_keys: Vec<i64> = Vec::new();
        let count = heap.scan_delete_matching(
            |data| {
                match crate::row::decode_column(&schema, &layout, data, 1) {
                    Value::Int(i) => i % 2 == 0,
                    _ => false,
                }
            },
            |_rid, data| {
                if let Value::Int(i) = crate::row::decode_column(&schema, &layout, data, 1) {
                    deleted_keys.push(i);
                }
            },
        ).unwrap();

        assert_eq!(count, 250); // half the rows
        assert_eq!(deleted_keys.len(), 250);
        deleted_keys.sort_unstable();
        let expected: Vec<i64> = (0..500).step_by(2).collect();
        assert_eq!(deleted_keys, expected);
        // Remaining rows should all be odd.
        let remaining: Vec<_> = heap.scan().collect();
        assert_eq!(remaining.len(), 250);
        for (_, data) in &remaining {
            let row = decode_row(&schema, data);
            if let Value::Int(i) = &row[1] {
                assert_eq!(i % 2, 1);
            }
        }
        drop(heap);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_scan_delete_matching_all_or_none() {
        let (mut heap, path) = temp_heap("sdm_edge");
        let schema = user_schema();
        for i in 0..50 {
            let row = vec![Value::Str(format!("u{i}")), Value::Int(i)];
            heap.insert(&encode_row(&schema, &row)).unwrap();
        }
        // Predicate never matches — zero deletions, scan count unchanged.
        let c = heap.scan_delete_matching(|_| false, |_rid, _| {}).unwrap();
        assert_eq!(c, 0);
        assert_eq!(heap.scan().count(), 50);

        // Predicate always matches — everything gone.
        let c = heap.scan_delete_matching(|_| true, |_rid, _| {}).unwrap();
        assert_eq!(c, 50);
        assert_eq!(heap.scan().count(), 0);
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
