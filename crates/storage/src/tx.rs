use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TX_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: u64,
    /// Snapshot: the set of tx_ids that were active when this tx began.
    active_at_start: HashSet<u64>,
    /// The tx_id counter value when this tx started (all tx < this existed).
    snapshot_id: u64,
}

impl Transaction {
    /// Can this transaction see data written by `writer_tx_id`?
    /// Visible if: writer committed before our snapshot AND wasn't active when we started.
    pub fn can_see(&self, writer_tx_id: u64) -> bool {
        if writer_tx_id == self.id {
            return true; // can always see own writes
        }
        // Must have started before us AND not been active when we started
        writer_tx_id < self.snapshot_id && !self.active_at_start.contains(&writer_tx_id)
    }
}

pub struct TxManager {
    active_txs: HashSet<u64>,
    committed_txs: HashSet<u64>,
    aborted_txs: HashSet<u64>,
}

impl TxManager {
    pub fn new() -> Self {
        TxManager {
            active_txs: HashSet::new(),
            committed_txs: HashSet::new(),
            aborted_txs: HashSet::new(),
        }
    }

    pub fn begin(&mut self) -> Transaction {
        let id = NEXT_TX_ID.fetch_add(1, Ordering::SeqCst);
        let snapshot_id = id;
        let active_at_start = self.active_txs.clone();
        self.active_txs.insert(id);
        Transaction { id, active_at_start, snapshot_id }
    }

    pub fn commit(&mut self, tx_id: u64) {
        self.active_txs.remove(&tx_id);
        self.committed_txs.insert(tx_id);
    }

    pub fn rollback(&mut self, tx_id: u64) {
        self.active_txs.remove(&tx_id);
        self.aborted_txs.insert(tx_id);
    }

    pub fn is_active(&self, tx_id: u64) -> bool {
        self.active_txs.contains(&tx_id)
    }

    pub fn is_aborted(&self, tx_id: u64) -> bool {
        self.aborted_txs.contains(&tx_id)
    }

    pub fn is_committed(&self, tx_id: u64) -> bool {
        self.committed_txs.contains(&tx_id)
    }

    /// The oldest active tx — undo entries before this are safe to purge.
    pub fn oldest_active(&self) -> Option<u64> {
        self.active_txs.iter().min().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::UndoLog;

    #[test]
    fn test_begin_commit() {
        let mut mgr = TxManager::new();
        let tx = mgr.begin();
        assert!(tx.id > 0);
        assert!(mgr.is_active(tx.id));
        mgr.commit(tx.id);
        assert!(!mgr.is_active(tx.id));
        assert!(mgr.is_committed(tx.id));
    }

    #[test]
    fn test_snapshot_isolation() {
        let mut mgr = TxManager::new();
        let tx1 = mgr.begin();
        let tx2 = mgr.begin();
        // tx1's snapshot should not see tx2's writes
        assert!(!tx1.can_see(tx2.id));
        // tx2's snapshot should not see tx1 (both active)
        assert!(!tx2.can_see(tx1.id));
        mgr.commit(tx1.id);
        // tx2 still shouldn't see tx1 (tx1 was active when tx2 started)
        assert!(!tx2.can_see(tx1.id));
    }

    #[test]
    fn test_sees_earlier_committed() {
        let mut mgr = TxManager::new();
        let tx1 = mgr.begin();
        mgr.commit(tx1.id);
        // tx2 starts after tx1 committed
        let tx2 = mgr.begin();
        assert!(tx2.can_see(tx1.id));
    }

    #[test]
    fn test_undo_log() {
        let mut undo = UndoLog::new();
        let ptr = undo.push(1, b"old version of row");
        let entry = undo.get(ptr).unwrap();
        assert_eq!(entry.tx_id, 1);
        assert_eq!(entry.data, b"old version of row");
    }

    #[test]
    fn test_undo_chain() {
        let mut undo = UndoLog::new();
        let ptr1 = undo.push_with_prev(1, b"version 1", None);
        let ptr2 = undo.push_with_prev(2, b"version 2", Some(ptr1));
        let entry2 = undo.get(ptr2).unwrap();
        assert_eq!(entry2.prev, Some(ptr1));
        let entry1 = undo.get(entry2.prev.unwrap()).unwrap();
        assert_eq!(entry1.data, b"version 1");
    }

    #[test]
    fn test_rollback() {
        let mut mgr = TxManager::new();
        let tx = mgr.begin();
        mgr.rollback(tx.id);
        assert!(!mgr.is_active(tx.id));
        assert!(mgr.is_aborted(tx.id));
    }

    #[test]
    fn test_oldest_active() {
        let mut mgr = TxManager::new();
        let tx1 = mgr.begin();
        let tx2 = mgr.begin();
        let _tx3 = mgr.begin();
        assert_eq!(mgr.oldest_active(), Some(tx1.id));
        mgr.commit(tx1.id);
        assert_eq!(mgr.oldest_active(), Some(tx2.id));
    }
}
