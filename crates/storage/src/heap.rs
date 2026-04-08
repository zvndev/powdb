use crate::disk::DiskManager;
use crate::page::{Page, PageType, PAGE_SIZE, iter_page_slots};
use crate::types::RowId;
use std::io;
use std::path::Path;

/// Manages a collection of data pages for storing rows.
/// Tracks which pages have free space for fast insertion.
pub struct HeapFile {
    disk: DiskManager,
    /// Pages with known free space.
    pages_with_space: Vec<u32>,
    /// Optional mmap for zero-syscall reads. Activated by `enable_mmap()`.
    mmap_ptr: Option<(*const u8, usize)>,
}

impl HeapFile {
    pub fn create(path: &Path) -> io::Result<Self> {
        let disk = DiskManager::create(path)?;
        Ok(HeapFile { disk, pages_with_space: Vec::new(), mmap_ptr: None })
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
        Ok(HeapFile { disk, pages_with_space, mmap_ptr: None })
    }

    /// Activate mmap for zero-syscall reads. Call after all inserts are done.
    /// The mmap covers the current file size; new inserts will invalidate it.
    pub fn enable_mmap(&mut self) {
        if self.mmap_ptr.is_some() {
            return;
        }
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
    pub fn insert(&mut self, row_data: &[u8]) -> io::Result<RowId> {
        // Try existing pages with space
        for idx in 0..self.pages_with_space.len() {
            let page_id = self.pages_with_space[idx];
            let buf = self.disk.read_page(page_id)?;
            let mut page = Page::from_bytes(&buf).unwrap();
            if let Some(slot) = page.insert(row_data) {
                self.disk.write_page(page_id, page.as_bytes())?;
                // Remove from free list if nearly full
                if page.free_space() < 64 {
                    self.pages_with_space.swap_remove(idx);
                }
                return Ok(RowId { page_id, slot_index: slot });
            }
        }
        // Allocate a new page
        let page_id = self.disk.allocate_page()?;
        let mut page = Page::new(page_id, PageType::Data);
        let slot = page.insert(row_data)
            .expect("row too large for empty page");
        self.disk.write_page(page_id, page.as_bytes())?;
        if page.free_space() >= 64 {
            self.pages_with_space.push(page_id);
        }
        Ok(RowId { page_id, slot_index: slot })
    }

    /// Read row data by RowId.
    ///
    /// Mission F: `#[inline]` so the mmap-fast-path branch can fold into
    /// `Catalog::get → Table::get → HeapFile::get` callsites. The hot path
    /// is the mmap branch — inlining lets LTO collapse the whole chain.
    #[inline]
    pub fn get(&self, rid: RowId) -> Option<Vec<u8>> {
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
    pub fn delete(&mut self, rid: RowId) -> io::Result<()> {
        let buf = self.disk.read_page(rid.page_id)?;
        let mut page = Page::from_bytes(&buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt page"))?;
        page.delete(rid.slot_index);
        self.disk.write_page(rid.page_id, page.as_bytes())?;
        if !self.pages_with_space.contains(&rid.page_id) {
            self.pages_with_space.push(rid.page_id);
        }
        Ok(())
    }

    /// Update a row. Returns new RowId (may change if row moves).
    pub fn update(&mut self, rid: RowId, row_data: &[u8]) -> io::Result<RowId> {
        if let Ok(buf) = self.disk.read_page(rid.page_id) {
            if let Some(mut page) = Page::from_bytes(&buf) {
                if page.update(rid.slot_index, row_data) {
                    self.disk.write_page(rid.page_id, page.as_bytes())?;
                    return Ok(rid);
                }
            }
        }
        // Doesn't fit in place — delete old, insert new
        self.delete(rid)?;
        self.insert(row_data)
    }

    /// Scan all live rows across all pages.
    pub fn scan(&self) -> impl Iterator<Item = (RowId, Vec<u8>)> + '_ {
        (0..self.disk.num_pages()).flat_map(move |page_id| {
            let entries: Vec<_> = self.disk.read_page(page_id).ok()
                .and_then(|buf| Page::from_bytes(&buf))
                .map(|page| {
                    page.iter().map(|(slot, data)| {
                        (RowId { page_id, slot_index: slot }, data.to_vec())
                    }).collect()
                })
                .unwrap_or_default();
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

        // Fast path: persistent mmap activated by `enable_mmap()`. Zero
        // syscalls per query — we just slice the existing mapping.
        if let Some((ptr, len)) = self.mmap_ptr {
            let mapped = unsafe { std::slice::from_raw_parts(ptr, len) };
            let pages_in_map = len / PAGE_SIZE;
            let limit = num_pages.min(pages_in_map as u32);
            'outer: for page_id in 0..limit {
                let offset = page_id as usize * PAGE_SIZE;
                let page_bytes = &mapped[offset..offset + PAGE_SIZE];
                for (slot, data) in iter_page_slots(page_bytes) {
                    if let ControlFlow::Break(()) = f(RowId { page_id, slot_index: slot }, data) {
                        break 'outer;
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
                let offset = page_id as usize * PAGE_SIZE;
                let page_bytes = &mapped[offset..offset + PAGE_SIZE];
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
    #[inline]
    pub fn for_each_row<F>(&self, mut f: F)
    where
        F: FnMut(RowId, &[u8]),
    {
        let num_pages = self.disk.num_pages();
        if num_pages == 0 {
            return;
        }

        // Fast path: persistent mmap.
        if let Some((ptr, len)) = self.mmap_ptr {
            let mapped = unsafe { std::slice::from_raw_parts(ptr, len) };
            let pages_in_map = len / PAGE_SIZE;
            let limit = num_pages.min(pages_in_map as u32);
            for page_id in 0..limit {
                let offset = page_id as usize * PAGE_SIZE;
                let page_bytes = &mapped[offset..offset + PAGE_SIZE];
                for (slot, data) in iter_page_slots(page_bytes) {
                    f(RowId { page_id, slot_index: slot }, data);
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
                let offset = page_id as usize * PAGE_SIZE;
                let page_bytes = &mapped[offset..offset + PAGE_SIZE];
                for (slot, data) in iter_page_slots(page_bytes) {
                    f(RowId { page_id, slot_index: slot }, data);
                }
            }
            unsafe { libc::munmap(ptr, file_len); }
        } else {
            // Fallback: per-page read
            for page_id in 0..num_pages {
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

    pub fn flush(&mut self) -> io::Result<()> {
        self.disk.flush()
    }
}

impl Drop for HeapFile {
    fn drop(&mut self) {
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
