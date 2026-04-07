use crate::page::PAGE_SIZE;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Manages page-level I/O to a single data file.
/// Each page is PAGE_SIZE bytes at offset = page_id * PAGE_SIZE.
pub struct DiskManager {
    file: File,
    num_pages: u32,
}

impl DiskManager {
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path)?;
        Ok(DiskManager { file, num_pages: 0 })
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true).write(true)
            .open(path)?;
        let len = file.metadata()?.len();
        let num_pages = (len / PAGE_SIZE as u64) as u32;
        Ok(DiskManager { file, num_pages })
    }

    /// Allocate a new page and extend the file. Returns the new page_id.
    pub fn allocate_page(&mut self) -> io::Result<u32> {
        let id = self.num_pages;
        let zeros = [0u8; PAGE_SIZE];
        let offset = id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&zeros)?;
        self.num_pages += 1;
        Ok(id)
    }

    pub fn write_page(&mut self, page_id: u32, data: &[u8]) -> io::Result<()> {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(())
    }

    pub fn read_page(&self, page_id: u32) -> io::Result<[u8; PAGE_SIZE]> {
        let mut buf = [0u8; PAGE_SIZE];
        let offset = page_id as u64 * PAGE_SIZE as u64;
        // Use a borrowed reference to avoid needing &mut self
        let mut file_ref = &self.file;
        file_ref.seek(SeekFrom::Start(offset))?;
        file_ref.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }

    pub fn num_pages(&self) -> u32 {
        self.num_pages
    }

    /// Borrow the underlying file (for mmap-based scans).
    pub fn file_ref(&self) -> &File {
        &self.file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{Page, PageType};
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("powdb_test_{name}_{}", std::process::id()))
    }

    #[test]
    fn test_create_and_read_page() {
        let path = temp_path("disk_basic");
        let mut dm = DiskManager::create(&path).unwrap();
        let page_id = dm.allocate_page().unwrap();
        assert_eq!(page_id, 0);

        let mut page = Page::new(page_id, PageType::Data);
        page.insert(b"hello disk");
        dm.write_page(page_id, page.as_bytes()).unwrap();

        let buf = dm.read_page(page_id).unwrap();
        let loaded = Page::from_bytes(&buf).unwrap();
        assert_eq!(loaded.get(0).unwrap(), b"hello disk");

        drop(dm);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_allocate_multiple_pages() {
        let path = temp_path("disk_multi");
        let mut dm = DiskManager::create(&path).unwrap();
        let p0 = dm.allocate_page().unwrap();
        let p1 = dm.allocate_page().unwrap();
        let p2 = dm.allocate_page().unwrap();
        assert_eq!(p0, 0);
        assert_eq!(p1, 1);
        assert_eq!(p2, 2);
        assert_eq!(dm.num_pages(), 3);

        drop(dm);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_reopen_file() {
        let path = temp_path("disk_reopen");
        {
            let mut dm = DiskManager::create(&path).unwrap();
            let id = dm.allocate_page().unwrap();
            let mut page = Page::new(id, PageType::Data);
            page.insert(b"persistent");
            dm.write_page(id, page.as_bytes()).unwrap();
            dm.flush().unwrap();
        }
        {
            let dm = DiskManager::open(&path).unwrap();
            assert_eq!(dm.num_pages(), 1);
            let buf = dm.read_page(0).unwrap();
            let page = Page::from_bytes(&buf).unwrap();
            assert_eq!(page.get(0).unwrap(), b"persistent");
        }
        std::fs::remove_file(&path).ok();
    }
}
