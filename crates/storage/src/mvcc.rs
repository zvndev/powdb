/// Pointer into the undo log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UndoPtr(pub usize);

/// A single undo log entry: the old version of a row before an update.
#[derive(Debug, Clone)]
pub struct UndoEntry {
    pub tx_id: u64,
    pub data: Vec<u8>,
    pub prev: Option<UndoPtr>, // previous version (undo chain)
}

/// Append-only undo log. Entries are never modified, only appended.
/// Old entries are reclaimed by advancing the purge watermark.
pub struct UndoLog {
    entries: Vec<UndoEntry>,
}

impl UndoLog {
    pub fn new() -> Self {
        UndoLog { entries: Vec::new() }
    }

    pub fn push(&mut self, tx_id: u64, data: &[u8]) -> UndoPtr {
        self.push_with_prev(tx_id, data, None)
    }

    pub fn push_with_prev(&mut self, tx_id: u64, data: &[u8], prev: Option<UndoPtr>) -> UndoPtr {
        let ptr = UndoPtr(self.entries.len());
        self.entries.push(UndoEntry {
            tx_id,
            data: data.to_vec(),
            prev,
        });
        ptr
    }

    pub fn get(&self, ptr: UndoPtr) -> Option<&UndoEntry> {
        self.entries.get(ptr.0)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
