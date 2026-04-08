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
        let node = self.nodes[node_id].clone();
        match node {
            Node::Leaf { mut keys, mut values, next_leaf } => {
                let pos = keys.iter().position(|k| k >= &key).unwrap_or(keys.len());

                // Duplicate key — update in place
                if pos < keys.len() && keys[pos] == key {
                    values[pos] = rid;
                    self.nodes[node_id] = Node::Leaf { keys, values, next_leaf };
                    return None;
                }

                keys.insert(pos, key);
                values.insert(pos, rid);

                if keys.len() <= ORDER {
                    self.nodes[node_id] = Node::Leaf { keys, values, next_leaf };
                    None
                } else {
                    // Split leaf
                    let mid = keys.len() / 2;
                    let right_keys = keys.split_off(mid);
                    let right_values = values.split_off(mid);
                    let right_id = self.nodes.len();
                    let mid_key = right_keys[0].clone();

                    self.nodes[node_id] = Node::Leaf { keys, values, next_leaf: Some(right_id) };
                    self.nodes.push(Node::Leaf { keys: right_keys, values: right_values, next_leaf });

                    Some((mid_key, right_id))
                }
            }
            Node::Internal { keys, children } => {
                let pos = keys.iter().position(|k| &key < k).unwrap_or(keys.len());
                let child_id = children[pos];

                if let Some((mid_key, new_child_id)) = self.insert_recursive(child_id, key, rid) {
                    // Child was split — insert the promoted key here
                    let node = &mut self.nodes[node_id];
                    if let Node::Internal { keys, children } = node {
                        keys.insert(pos, mid_key.clone());
                        children.insert(pos + 1, new_child_id);

                        if keys.len() <= ORDER {
                            return None;
                        }

                        // Split internal node
                        let mid = keys.len() / 2;
                        let promote_key = keys[mid].clone();
                        let right_keys: Vec<Value> = keys.drain(mid + 1..).collect();
                        keys.truncate(mid);
                        let right_children: Vec<usize> = children.drain(mid + 1..).collect();

                        let right_id = self.nodes.len();
                        self.nodes.push(Node::Internal {
                            keys: right_keys,
                            children: right_children,
                        });
                        return Some((promote_key, right_id));
                    }
                    unreachable!()
                } else {
                    None
                }
            }
        }
    }

    /// Point lookup: find the RowId for a given key.
    ///
    /// Mission F: `#[inline]` lets LTO fold this into Engine::index_lookup
    /// fast paths so the call frame doesn't dominate single-row reads.
    /// (D1 will replace the linear scans with binary search, but inlining
    /// already helps even in the linear case.)
    #[inline]
    pub fn lookup(&self, key: &Value) -> Option<RowId> {
        let mut node_id = self.root;
        loop {
            match &self.nodes[node_id] {
                Node::Leaf { keys, values, .. } => {
                    return keys.iter().position(|k| k == key)
                        .map(|i| values[i]);
                }
                Node::Internal { keys, children } => {
                    let pos = keys.iter().position(|k| key < k).unwrap_or(keys.len());
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
                    if let Some(pos) = keys.iter().position(|k| k == key) {
                        keys.remove(pos);
                        values.remove(pos);
                        return true;
                    }
                }
                return false;
            }
            match &self.nodes[node_id] {
                Node::Internal { keys, children } => {
                    let pos = keys.iter().position(|k| key < k).unwrap_or(keys.len());
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
                    let pos = keys.iter().position(|k| start < k).unwrap_or(keys.len());
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
