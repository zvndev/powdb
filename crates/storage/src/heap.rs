use crate::disk::DiskManager;
use crate::page::{Page, PageType};
use crate::types::RowId;
use std::io;
use std::path::Path;

/// Manages a collection of data pages for storing rows.
/// Tracks which pages have free space for fast insertion.
pub struct HeapFile {
    disk: DiskManager,
    /// Pages with known free space.
    pages_with_space: Vec<u32>,
}

impl HeapFile {
    pub fn create(path: &Path) -> io::Result<Self> {
        let disk = DiskManager::create(path)?;
        Ok(HeapFile { disk, pages_with_space: Vec::new() })
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
        Ok(HeapFile { disk, pages_with_space })
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
    pub fn get(&self, rid: RowId) -> Option<Vec<u8>> {
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

    pub fn flush(&mut self) -> io::Result<()> {
        self.disk.flush()
    }
}

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
        let path = std::env::temp_dir().join(format!("batadb_heap_{name}_{}", std::process::id()));
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
