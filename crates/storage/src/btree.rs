use crate::types::{RowId, Value};
use std::path::Path;

const ORDER: usize = 256;

#[derive(Debug, Clone)]
enum Node {
    Internal {
        keys: Vec<Value>,
        children: Vec<usize>, // indices into nodes vec
    },
    Leaf {
        keys: Vec<Value>,
        values: Vec<RowId>,
        next_leaf: Option<usize>,
    },
}

/// In-memory B+ tree index. Keys are Values, values are RowIds.
///
/// Order 256: each node holds up to 256 keys. For 500K rows with integer keys,
/// this gives height 3 (3 node visits per lookup). For 1 billion rows: height 4.
pub struct BTree {
    nodes: Vec<Node>,
    root: usize,
    #[allow(dead_code)]
    path: std::path::PathBuf,
}

impl BTree {
    pub fn create(path: &Path) -> std::io::Result<Self> {
        let root_node = Node::Leaf {
            keys: Vec::new(),
            values: Vec::new(),
            next_leaf: None,
        };
        Ok(BTree {
            nodes: vec![root_node],
            root: 0,
            path: path.to_path_buf(),
        })
    }

    pub fn insert(&mut self, key: Value, rid: RowId) {
        let root = self.root;
        if let Some((mid_key, new_node_id)) = self.insert_recursive(root, key, rid) {
            // Root was split — create new root
            let new_root = Node::Internal {
                keys: vec![mid_key],
                children: vec![self.root, new_node_id],
            };
            let new_root_id = self.nodes.len();
            self.nodes.push(new_root);
            self.root = new_root_id;
        }
    }

    fn insert_recursive(&mut self, node_id: usize, key: Value, rid: RowId) -> Option<(Value, usize)> {
        // Mission C Phase 6: in-place insert.
        //
        // The previous implementation did `let node = self.nodes[node_id].clone();`
        // at the top of every recursive call. For the common int-keyed leaf
        // that's a Vec<Value> of up to 256 entries + a Vec<RowId> of the same
        // length — roughly 4-6 KB of memcpy per insert recursion. With a
        // height-3 tree that's 12-18 KB of allocator + memcpy traffic on
        // every insert, which on a 100K-row bench loop dominates the whole
        // operation.
        //
        // The rewrite below does three things:
        //   1. **Hot path (leaf, no split):** a single `&mut self.nodes[node_id]`
        //      match, binary search, `Vec::insert` — zero clones.
        //   2. **Leaf split:** still in place; the only allocation is the
        //      new right-leaf Node we push onto `self.nodes`.
        //   3. **Internal descend:** reads `pos` and `child_id` under a
        //      short borrow, drops the borrow, recurses, then re-borrows to
        //      insert the promoted key. No node-level clone anywhere.
        match &mut self.nodes[node_id] {
            Node::Leaf { keys, values, .. } => {
                let pos = keys.partition_point(|k| k < &key);

                // Duplicate key — update in place.
                if pos < keys.len() && keys[pos] == key {
                    values[pos] = rid;
                    return None;
                }

                keys.insert(pos, key);
                values.insert(pos, rid);

                if keys.len() <= ORDER {
                    return None;
                }

                // Overflow — split. Do the split work while we still hold
                // the borrow on the current leaf, capture the right-half
                // buffers + mid_key, drop the borrow, then push the new
                // leaf onto `self.nodes` and fix up the left leaf's
                // `next_leaf` pointer.
                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid);
                let right_values = values.split_off(mid);
                let mid_key = right_keys[0].clone();
                // The borrow on self.nodes[node_id] ends here.
                let captured_next_leaf = match &self.nodes[node_id] {
                    Node::Leaf { next_leaf, .. } => *next_leaf,
                    _ => unreachable!(),
                };

                let right_id = self.nodes.len();
                self.nodes.push(Node::Leaf {
                    keys: right_keys,
                    values: right_values,
                    next_leaf: captured_next_leaf,
                });
                if let Node::Leaf { next_leaf, .. } = &mut self.nodes[node_id] {
                    *next_leaf = Some(right_id);
                }

                Some((mid_key, right_id))
            }
            Node::Internal { keys, children } => {
                // Pick the child whose separator is strictly greater than
                // `key`. We only need `pos` and `child_id` — drop the borrow
                // before recursing.
                let pos = keys.partition_point(|k| k <= &key);
                let child_id = children[pos];
                // Borrow on self.nodes[node_id] ends here.

                let Some((mid_key, new_child_id)) = self.insert_recursive(child_id, key, rid) else {
                    return None;
                };

                // Re-borrow to insert the promoted key; possibly split this
                // internal node. All work that needs the borrow happens
                // inside the match arm; `self.nodes.push` for the split
                // right-half runs after the borrow drops.
                let split_payload = match &mut self.nodes[node_id] {
                    Node::Internal { keys, children } => {
                        keys.insert(pos, mid_key);
                        children.insert(pos + 1, new_child_id);
                        if keys.len() <= ORDER {
                            None
                        } else {
                            let mid = keys.len() / 2;
                            let promote_key = keys[mid].clone();
                            let right_keys: Vec<Value> = keys.drain(mid + 1..).collect();
                            keys.truncate(mid);
                            let right_children: Vec<usize> = children.drain(mid + 1..).collect();
                            Some((promote_key, right_keys, right_children))
                        }
                    }
                    _ => unreachable!(),
                };

                if let Some((promote_key, right_keys, right_children)) = split_payload {
                    let right_id = self.nodes.len();
                    self.nodes.push(Node::Internal {
                        keys: right_keys,
                        children: right_children,
                    });
                    Some((promote_key, right_id))
                } else {
                    None
                }
            }
        }
    }

    /// Point lookup: find the RowId for a given key.
    ///
    /// Mission D1: binary search instead of linear scan. With ORDER=256 nodes,
    /// linear scan was ~128 comparisons average; binary search is ~8. The
    /// `Value` Ord impl is total, so `binary_search` is sound. Mission F's
    /// `#[inline]` is preserved so LTO can still fold this into the index-
    /// lookup fast path.
    #[inline]
    pub fn lookup(&self, key: &Value) -> Option<RowId> {
        let mut node_id = self.root;
        loop {
            match &self.nodes[node_id] {
                Node::Leaf { keys, values, .. } => {
                    return match keys.binary_search(key) {
                        Ok(i) => Some(values[i]),
                        Err(_) => None,
                    };
                }
                Node::Internal { keys, children } => {
                    // First child whose separator is strictly greater than `key`.
                    // `partition_point(p)` returns the first index where `p` is
                    // false, so `|k| k <= key` finds the first `k > key`.
                    let pos = keys.partition_point(|k| k <= key);
                    node_id = children[pos];
                }
            }
        }
    }

    /// Mission D7: specialized int-keyed point lookup.
    ///
    /// For an int-keyed index (the overwhelming common case — primary keys,
    /// foreign keys, `created_at` timestamps), every comparison inside
    /// `lookup` goes through `<Value as Ord>::cmp`, which matches on the
    /// discriminant of **both** sides before forwarding to `i64::cmp`. Even
    /// with `#[inline]` that's 5-10ns of pure dispatch per comparison. With
    /// binary search on an order-256 B+tree of ~100K rows we do ~24
    /// comparisons per lookup — that's 120-240ns of overhead on top of the
    /// actual work. On the 124ns `point_lookup_indexed` measurement that's
    /// essentially all the cost.
    ///
    /// This fast path:
    ///   1. Takes the key as a raw `i64` (no `Value::Int` allocation).
    ///   2. At every comparison, extracts the stored `i64` directly via a
    ///      single-sided match, cutting out half of the dispatch.
    ///   3. Uses `debug_unreachable!`-style fallback for non-int keys — the
    ///      caller is expected to only call this on an int-keyed index.
    ///
    /// Callers that are unsure of the index type should use `lookup` instead;
    /// the old path remains correct for every type.
    #[inline]
    pub fn lookup_int(&self, key: i64) -> Option<RowId> {
        let mut node_id = self.root;
        loop {
            match &self.nodes[node_id] {
                Node::Leaf { keys, values, .. } => {
                    // Binary search with single-sided discriminant match.
                    // On a well-typed int index this compiles down to a
                    // straight `i64::cmp` loop because LLVM speculates the
                    // match arm.
                    let result = keys.binary_search_by(|k| match k {
                        Value::Int(i) => i.cmp(&key),
                        _ => std::cmp::Ordering::Less,
                    });
                    return match result {
                        Ok(i) => Some(values[i]),
                        Err(_) => None,
                    };
                }
                Node::Internal { keys, children } => {
                    // First child whose separator is strictly greater than `key`.
                    let pos = keys.partition_point(|k| match k {
                        Value::Int(i) => *i <= key,
                        _ => false,
                    });
                    node_id = children[pos];
                }
            }
        }
    }

    /// Delete a key from the tree. Returns true if the key was found and removed.
    pub fn delete(&mut self, key: &Value) -> bool {
        // Simple deletion: find leaf and remove (no rebalancing for now — acceptable
        // for initial implementation, tree stays valid just potentially underfull)
        let mut node_id = self.root;
        loop {
            let is_leaf = matches!(self.nodes[node_id], Node::Leaf { .. });
            if is_leaf {
                if let Node::Leaf { keys, values, .. } = &mut self.nodes[node_id] {
                    // Mission D1: binary search the leaf for an exact match.
                    if let Ok(pos) = keys.binary_search(key) {
                        keys.remove(pos);
                        values.remove(pos);
                        return true;
                    }
                }
                return false;
            }
            match &self.nodes[node_id] {
                Node::Internal { keys, children } => {
                    // Mission D1: binary search for child descent.
                    let pos = keys.partition_point(|k| k <= key);
                    node_id = children[pos];
                }
                _ => unreachable!(),
            }
        }
    }

    /// Range scan: returns all (key, rid) pairs where start <= key <= end.
    pub fn range<'a>(&'a self, start: &Value, end: &Value) -> impl Iterator<Item = (Value, RowId)> + 'a {
        // Find the leaf containing `start`
        let mut node_id = self.root;
        loop {
            match &self.nodes[node_id] {
                Node::Internal { keys, children } => {
                    // Mission D1: binary search for child descent.
                    let pos = keys.partition_point(|k| k <= start);
                    node_id = children[pos];
                }
                Node::Leaf { .. } => break,
            }
        }

        // Walk leaf chain collecting results
        let end = end.clone();
        let start = start.clone();
        let mut results = Vec::new();
        let mut current = Some(node_id);
        while let Some(nid) = current {
            match &self.nodes[nid] {
                Node::Leaf { keys, values, next_leaf } => {
                    let mut done = false;
                    for (i, k) in keys.iter().enumerate() {
                        if k > &end {
                            done = true;
                            break;
                        }
                        if k >= &start {
                            results.push((k.clone(), values[i]));
                        }
                    }
                    if done {
                        break;
                    }
                    current = *next_leaf;
                }
                _ => break,
            }
        }
        results.into_iter()
    }

    /// Number of entries in the tree.
    pub fn len(&self) -> usize {
        let mut count = 0;
        let mut node_id = self.root;
        // Find leftmost leaf
        loop {
            match &self.nodes[node_id] {
                Node::Leaf { .. } => break,
                Node::Internal { children, .. } => {
                    node_id = children[0];
                }
            }
        }
        // Walk leaf chain
        let mut current = Some(node_id);
        while let Some(nid) = current {
            match &self.nodes[nid] {
                Node::Leaf { keys, next_leaf, .. } => {
                    count += keys.len();
                    current = *next_leaf;
                }
                _ => break,
            }
        }
        count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_btree(name: &str) -> BTree {
        let path = std::env::temp_dir().join(format!("powdb_btree_{name}_{}", std::process::id()));
        BTree::create(&path).unwrap()
    }

    #[test]
    fn test_insert_and_lookup() {
        let mut bt = temp_btree("basic");
        let rid = RowId { page_id: 1, slot_index: 0 };
        bt.insert(Value::Int(42), rid);
        assert_eq!(bt.lookup(&Value::Int(42)), Some(rid));
        assert_eq!(bt.lookup(&Value::Int(99)), None);
    }

    #[test]
    fn test_many_inserts_and_lookups() {
        let mut bt = temp_btree("many");
        for i in 0..1000 {
            bt.insert(Value::Int(i), RowId {
                page_id: (i / 100) as u32,
                slot_index: (i % 100) as u16,
            });
        }
        assert_eq!(bt.len(), 1000);
        for i in 0..1000 {
            let rid = bt.lookup(&Value::Int(i)).unwrap_or_else(|| panic!("key {i} missing"));
            assert_eq!(rid.page_id, (i / 100) as u32);
            assert_eq!(rid.slot_index, (i % 100) as u16);
        }
    }

    #[test]
    fn test_range_scan() {
        let mut bt = temp_btree("range");
        for i in 0..100 {
            bt.insert(Value::Int(i), RowId { page_id: 0, slot_index: i as u16 });
        }
        let results: Vec<_> = bt.range(&Value::Int(10), &Value::Int(20)).collect();
        assert_eq!(results.len(), 11); // 10..=20 inclusive
        assert_eq!(results[0].0, Value::Int(10));
        assert_eq!(results[10].0, Value::Int(20));
    }

    #[test]
    fn test_string_keys() {
        let mut bt = temp_btree("strings");
        bt.insert(Value::Str("alice".into()), RowId { page_id: 0, slot_index: 0 });
        bt.insert(Value::Str("bob".into()), RowId { page_id: 0, slot_index: 1 });
        bt.insert(Value::Str("charlie".into()), RowId { page_id: 0, slot_index: 2 });
        assert_eq!(bt.lookup(&Value::Str("bob".into())).unwrap().slot_index, 1);
        assert_eq!(bt.lookup(&Value::Str("dave".into())), None);
    }

    #[test]
    fn test_delete() {
        let mut bt = temp_btree("delete");
        bt.insert(Value::Int(1), RowId { page_id: 0, slot_index: 0 });
        bt.insert(Value::Int(2), RowId { page_id: 0, slot_index: 1 });
        assert!(bt.delete(&Value::Int(1)));
        assert_eq!(bt.lookup(&Value::Int(1)), None);
        assert_eq!(bt.lookup(&Value::Int(2)).unwrap().slot_index, 1);
        assert_eq!(bt.len(), 1);
    }

    #[test]
    fn test_duplicate_key_updates() {
        let mut bt = temp_btree("dup");
        bt.insert(Value::Int(42), RowId { page_id: 0, slot_index: 0 });
        bt.insert(Value::Int(42), RowId { page_id: 1, slot_index: 5 });
        // Should update, not duplicate
        assert_eq!(bt.len(), 1);
        assert_eq!(bt.lookup(&Value::Int(42)).unwrap(), RowId { page_id: 1, slot_index: 5 });
    }

    #[test]
    fn test_large_tree_splits() {
        let mut bt = temp_btree("large");
        // Insert enough to force multiple splits
        for i in 0..5000 {
            bt.insert(Value::Int(i), RowId { page_id: (i / 256) as u32, slot_index: (i % 256) as u16 });
        }
        assert_eq!(bt.len(), 5000);
        // Verify all entries
        for i in 0..5000 {
            assert!(bt.lookup(&Value::Int(i)).is_some(), "key {i} not found after splits");
        }
        // Range scan across splits
        let results: Vec<_> = bt.range(&Value::Int(2000), &Value::Int(3000)).collect();
        assert_eq!(results.len(), 1001);
    }

    #[test]
    fn test_lookup_int_matches_lookup() {
        // Mission D7: specialized int path must return identical results
        // to the generic `lookup` for every key, present or absent, across
        // a tree large enough to exercise multiple levels + splits.
        let mut bt = temp_btree("lookup_int");
        for i in 0..5000 {
            bt.insert(Value::Int(i), RowId {
                page_id: (i / 256) as u32,
                slot_index: (i % 256) as u16,
            });
        }
        for i in -5..5005 {
            let generic = bt.lookup(&Value::Int(i));
            let specialized = bt.lookup_int(i);
            assert_eq!(generic, specialized, "divergence at key {i}");
        }
    }

    #[test]
    fn test_reverse_insert_order() {
        let mut bt = temp_btree("reverse");
        for i in (0..500).rev() {
            bt.insert(Value::Int(i), RowId { page_id: 0, slot_index: i as u16 });
        }
        assert_eq!(bt.len(), 500);
        // Range scan should return sorted order
        let results: Vec<_> = bt.range(&Value::Int(0), &Value::Int(499)).collect();
        for (j, (k, _)) in results.iter().enumerate() {
            assert_eq!(*k, Value::Int(j as i64));
        }
    }
}
