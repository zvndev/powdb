pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 8;
const SLOT_COUNT_SIZE: usize = 2;    // u16 at bottom of page
const SLOT_ENTRY_SIZE: usize = 4;    // u16 offset + u16 length per slot
const DELETED_MARKER: u16 = 0xFFFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    Data     = 1,
    Index    = 2,
    Overflow = 3,
    Wal      = 4,
    Meta     = 5,
}

impl PageType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(PageType::Data),
            2 => Some(PageType::Index),
            3 => Some(PageType::Overflow),
            4 => Some(PageType::Wal),
            5 => Some(PageType::Meta),
            _ => None,
        }
    }
}

/// A 4KB page with header, row data growing down, slot directory growing up.
///
/// Layout:
///   [0..8]        Header: page_id(u32) + page_type(u8) + flags(u8) + free_start(u16)
///   [8..free_start] Row data (grows downward from header)
///   [free_start..dir_bottom] Free space
///   [dir_bottom..4094] Slot directory (grows upward): each entry is offset(u16) + length(u16)
///   [4094..4096]  slot_count(u16)
#[derive(Clone)]
pub struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    /// Create a fresh empty page.
    pub fn new(page_id: u32, page_type: PageType) -> Self {
        let mut data = [0u8; PAGE_SIZE];
        data[0..4].copy_from_slice(&page_id.to_le_bytes());
        data[4] = page_type as u8;
        data[5] = 0; // flags
        let free_start = PAGE_HEADER_SIZE as u16;
        data[6..8].copy_from_slice(&free_start.to_le_bytes());
        // slot_count = 0
        data[PAGE_SIZE - 2..PAGE_SIZE].copy_from_slice(&0u16.to_le_bytes());
        Page { data }
    }

    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() != PAGE_SIZE {
            return None;
        }
        let mut data = [0u8; PAGE_SIZE];
        data.copy_from_slice(buf);
        Some(Page { data })
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.data
    }

    pub fn page_id(&self) -> u32 {
        u32::from_le_bytes(self.data[0..4].try_into().unwrap())
    }

    pub fn page_type(&self) -> PageType {
        PageType::from_u8(self.data[4]).unwrap()
    }

    fn free_start(&self) -> u16 {
        u16::from_le_bytes(self.data[6..8].try_into().unwrap())
    }

    fn set_free_start(&mut self, v: u16) {
        self.data[6..8].copy_from_slice(&v.to_le_bytes());
    }

    pub fn slot_count(&self) -> u16 {
        u16::from_le_bytes(self.data[PAGE_SIZE - 2..PAGE_SIZE].try_into().unwrap())
    }

    fn set_slot_count(&mut self, v: u16) {
        self.data[PAGE_SIZE - 2..PAGE_SIZE].copy_from_slice(&v.to_le_bytes());
    }

    /// Byte offset where slot entry `i` starts in the page.
    /// Slot directory grows upward from the bottom (before slot_count).
    fn slot_entry_offset(&self, i: u16) -> usize {
        PAGE_SIZE - SLOT_COUNT_SIZE - ((i as usize + 1) * SLOT_ENTRY_SIZE)
    }

    fn read_slot_entry(&self, i: u16) -> (u16, u16) {
        let off = self.slot_entry_offset(i);
        let offset = u16::from_le_bytes(self.data[off..off + 2].try_into().unwrap());
        let length = u16::from_le_bytes(self.data[off + 2..off + 4].try_into().unwrap());
        (offset, length)
    }

    fn write_slot_entry(&mut self, i: u16, offset: u16, length: u16) {
        let off = self.slot_entry_offset(i);
        self.data[off..off + 2].copy_from_slice(&offset.to_le_bytes());
        self.data[off + 2..off + 4].copy_from_slice(&length.to_le_bytes());
    }

    /// Available free space for new data + a new slot entry.
    pub fn free_space(&self) -> usize {
        let data_end = self.free_start() as usize;
        let dir_start = if self.slot_count() == 0 {
            PAGE_SIZE - SLOT_COUNT_SIZE
        } else {
            self.slot_entry_offset(self.slot_count() - 1)
        };
        dir_start.saturating_sub(data_end)
    }

    /// Insert data into the page. Returns slot index, or None if not enough space.
    pub fn insert(&mut self, row_data: &[u8]) -> Option<u16> {
        let needed = row_data.len() + SLOT_ENTRY_SIZE;
        if needed > self.free_space() {
            return None;
        }
        let slot_idx = self.slot_count();
        let offset = self.free_start();

        // Write row data
        let start = offset as usize;
        let end = start + row_data.len();
        self.data[start..end].copy_from_slice(row_data);

        // Write slot entry
        self.write_slot_entry(slot_idx, offset, row_data.len() as u16);

        // Update header
        self.set_free_start(end as u16);
        self.set_slot_count(slot_idx + 1);

        Some(slot_idx)
    }

    /// Read data at slot index. Returns None if slot is deleted or out of range.
    pub fn get(&self, slot: u16) -> Option<&[u8]> {
        if slot >= self.slot_count() {
            return None;
        }
        let (offset, length) = self.read_slot_entry(slot);
        if length == DELETED_MARKER {
            return None;
        }
        let start = offset as usize;
        let end = start + length as usize;
        Some(&self.data[start..end])
    }

    /// Mark a slot as deleted. Does not reclaim space (compaction is separate).
    pub fn delete(&mut self, slot: u16) {
        if slot < self.slot_count() {
            let (offset, _) = self.read_slot_entry(slot);
            self.write_slot_entry(slot, offset, DELETED_MARKER);
        }
    }

    /// Update data in a slot in place if it fits, otherwise append at free_start.
    pub fn update(&mut self, slot: u16, row_data: &[u8]) -> bool {
        if slot >= self.slot_count() {
            return false;
        }
        let (offset, old_length) = self.read_slot_entry(slot);
        if old_length == DELETED_MARKER {
            return false;
        }
        if row_data.len() <= old_length as usize {
            let start = offset as usize;
            self.data[start..start + row_data.len()].copy_from_slice(row_data);
            self.write_slot_entry(slot, offset, row_data.len() as u16);
            true
        } else {
            // Need more space — append at free_start
            if row_data.len() > self.free_space() {
                return false;
            }
            let new_offset = self.free_start();
            let start = new_offset as usize;
            self.data[start..start + row_data.len()].copy_from_slice(row_data);
            self.write_slot_entry(slot, new_offset, row_data.len() as u16);
            self.set_free_start((start + row_data.len()) as u16);
            true
        }
    }

    /// Iterate over all live (non-deleted) slots. Returns (slot_index, data).
    pub fn iter(&self) -> impl Iterator<Item = (u16, &[u8])> {
        (0..self.slot_count()).filter_map(move |i| {
            self.get(i).map(|data| (i, data))
        })
    }
}

/// Iterate live slots directly from a page-sized byte slice without copying.
/// Used by mmap-based scans to avoid the 4KB memcpy in `Page::from_bytes`.
pub fn iter_page_slots(page_bytes: &[u8]) -> impl Iterator<Item = (u16, &[u8])> {
    let slot_count = u16::from_le_bytes(
        page_bytes[PAGE_SIZE - 2..PAGE_SIZE].try_into().unwrap(),
    );
    (0..slot_count).filter_map(move |i| {
        let entry_off = PAGE_SIZE - SLOT_COUNT_SIZE - ((i as usize + 1) * SLOT_ENTRY_SIZE);
        let offset = u16::from_le_bytes(
            page_bytes[entry_off..entry_off + 2].try_into().unwrap(),
        );
        let length = u16::from_le_bytes(
            page_bytes[entry_off + 2..entry_off + 4].try_into().unwrap(),
        );
        if length == DELETED_MARKER {
            return None;
        }
        let start = offset as usize;
        let end = start + length as usize;
        Some((i, &page_bytes[start..end]))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page() {
        let page = Page::new(0, PageType::Data);
        assert_eq!(page.page_id(), 0);
        assert_eq!(page.page_type(), PageType::Data);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.free_space(), PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_COUNT_SIZE);
    }

    #[test]
    fn test_insert_and_read_slot() {
        let mut page = Page::new(1, PageType::Data);
        let data = b"hello world";
        let slot = page.insert(data).expect("insert should succeed");
        assert_eq!(slot, 0);
        assert_eq!(page.slot_count(), 1);
        assert_eq!(page.get(0).unwrap(), data);
    }

    #[test]
    fn test_multiple_inserts() {
        let mut page = Page::new(1, PageType::Data);
        let s0 = page.insert(b"first").unwrap();
        let s1 = page.insert(b"second").unwrap();
        let s2 = page.insert(b"third").unwrap();
        assert_eq!(s0, 0);
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        assert_eq!(page.get(0).unwrap(), b"first");
        assert_eq!(page.get(1).unwrap(), b"second");
        assert_eq!(page.get(2).unwrap(), b"third");
    }

    #[test]
    fn test_page_full() {
        let mut page = Page::new(1, PageType::Data);
        let big = vec![0u8; PAGE_SIZE];
        assert!(page.insert(&big).is_none());
    }

    #[test]
    fn test_delete_slot() {
        let mut page = Page::new(1, PageType::Data);
        page.insert(b"keep");
        page.insert(b"delete me");
        page.insert(b"keep too");
        page.delete(1);
        assert!(page.get(1).is_none());
        assert_eq!(page.get(0).unwrap(), b"keep");
        assert_eq!(page.get(2).unwrap(), b"keep too");
    }

    #[test]
    fn test_page_serialization_roundtrip() {
        let mut page = Page::new(42, PageType::Data);
        page.insert(b"hello");
        page.insert(b"world");
        let buf = page.as_bytes();
        assert_eq!(buf.len(), PAGE_SIZE);
        let page2 = Page::from_bytes(buf).unwrap();
        assert_eq!(page2.page_id(), 42);
        assert_eq!(page2.slot_count(), 2);
        assert_eq!(page2.get(0).unwrap(), b"hello");
        assert_eq!(page2.get(1).unwrap(), b"world");
    }

    #[test]
    fn test_update_in_place() {
        let mut page = Page::new(1, PageType::Data);
        page.insert(b"hello world!!");
        assert!(page.update(0, b"hi world")); // smaller — fits in place
        assert_eq!(page.get(0).unwrap(), b"hi world");
    }

    #[test]
    fn test_update_larger_appends() {
        let mut page = Page::new(1, PageType::Data);
        page.insert(b"hi");
        let free_before = page.free_space();
        assert!(page.update(0, b"hello world much longer")); // larger — appends
        assert_eq!(page.get(0).unwrap(), b"hello world much longer");
        assert!(page.free_space() < free_before);
    }

    #[test]
    fn test_iter_skips_deleted() {
        let mut page = Page::new(1, PageType::Data);
        page.insert(b"a");
        page.insert(b"b");
        page.insert(b"c");
        page.delete(1);
        let live: Vec<_> = page.iter().collect();
        assert_eq!(live.len(), 2);
        assert_eq!(live[0], (0, &b"a"[..]));
        assert_eq!(live[1], (2, &b"c"[..]));
    }

    #[test]
    fn test_fill_page_to_capacity() {
        let mut page = Page::new(0, PageType::Data);
        let mut count = 0u16;
        // Insert 10-byte rows until full
        while page.insert(&[0u8; 10]).is_some() {
            count += 1;
        }
        // 4096 - 8 (header) - 2 (slot_count) = 4086 usable
        // Each row: 10 data + 4 slot entry = 14 bytes
        // 4086 / 14 = 291 rows
        assert!(count > 280 && count <= 292, "expected ~291 rows, got {count}");
        assert_eq!(page.slot_count(), count);
    }
}
