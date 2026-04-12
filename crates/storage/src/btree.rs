use crate::types::{RowId, TypeId, Value};
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;

const ORDER: usize = 256;

// ─── On-disk btree file format ────────────────────────────────────────────
//
// Mission 3: persist b-tree indexes to disk so `CREATE INDEX` survives a
// restart. Format is a small hand-rolled little-endian blob — we explicitly
// avoid pulling in a new serializer dep (no `serde`/`bincode`/`postcard` in
// the workspace, and the tree's node shape is simple enough to encode by
// hand).
//
// Layout:
//   magic     [4]  = "BIDX"
//   version   u16
//   root      u32
//   n_nodes   u32
//   for each node:
//     tag     u8   (0 = Internal, 1 = Leaf)
//     n_keys  u32
//     for each key: encoded Value (type tag + payload — see write_value/read_value)
//     if Internal:
//       n_children u32
//       children:  u32 * n_children
//     if Leaf:
//       for each value slot: page_id u32 + slot_index u16
//       next_leaf_present u8 (0/1)
//       if present: next_leaf u32
//
// Value encoding:
//   type_id u8 + payload. For Int/Float/DateTime: 8 bytes LE. For Bool: 1
//   byte. For Str: u32 len + UTF-8 bytes. For Uuid: 16 bytes. For Bytes:
//   u32 len + raw bytes. For Empty: no payload.
const BTREE_MAGIC: &[u8; 4] = b"BIDX";
const BTREE_VERSION: u16 = 1;
const NODE_TAG_INTERNAL: u8 = 0;
const NODE_TAG_LEAF: u8 = 1;

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
    /// Backing file for on-disk persistence. Set at `create`/`load` time;
    /// `save` writes to this path atomically (write-then-rename).
    path: std::path::PathBuf,
    /// Blocker B3: has this tree been mutated in memory since the last
    /// `save_if_dirty` / `save` call? Every mutating method flips this to
    /// `true`; `save_if_dirty` skips the serialize+rename when the flag
    /// is clear, so a checkpoint that touches an untouched tree is free.
    /// Set to `false` on `create` / `load` / any successful `save_to`.
    dirty: bool,
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
            // Fresh trees have no on-disk content yet; the caller is
            // expected to `save` once after bulk-loading before the
            // dirty flag is meaningful.
            dirty: false,
        })
    }

    pub fn insert(&mut self, key: Value, rid: RowId) {
        // Blocker B3: any mutation flips the dirty flag. Callers
        // (Table::insert, Table::update_hinted, ...) defer the actual
        // disk write to the next `save_if_dirty` — typically at
        // `Catalog::checkpoint` or on `Drop`.
        self.dirty = true;
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

    /// Mission C Phase 15: specialised int-keyed insert.
    ///
    /// Same rationale as `lookup_int` / `delete_int`: every comparison in
    /// the generic path runs through `<Value as Ord>::cmp`, which matches
    /// on both sides' discriminants before forwarding to `i64::cmp`. On
    /// `insert_batch_1k` (1000 rows, one index on `id`, ~8 comparisons
    /// per descent) that's enough dispatch traffic to show up in the
    /// bench. This path takes the key as a raw `i64`, does single-sided
    /// discriminant matching in the binary-search loop, and stores the
    /// key as `Value::Int(i64)` so the on-disk representation stays
    /// compatible with the generic `insert` / `lookup` paths.
    #[inline]
    pub fn insert_int(&mut self, key: i64, rid: RowId) {
        // Blocker B3: mark dirty; checkpoint flushes later.
        self.dirty = true;
        let root = self.root;
        if let Some((mid_key, new_node_id)) = self.insert_recursive_int(root, key, rid) {
            let new_root = Node::Internal {
                keys: vec![Value::Int(mid_key)],
                children: vec![self.root, new_node_id],
            };
            let new_root_id = self.nodes.len();
            self.nodes.push(new_root);
            self.root = new_root_id;
        }
    }

    fn insert_recursive_int(&mut self, node_id: usize, key: i64, rid: RowId) -> Option<(i64, usize)> {
        match &mut self.nodes[node_id] {
            Node::Leaf { keys, values, .. } => {
                // Single-sided i64 comparison: since the leaf is all
                // Value::Int (invariant of an int index), LLVM collapses
                // this to straight `i64 < key` after inlining.
                let pos = keys.partition_point(|k| match k {
                    Value::Int(i) => *i < key,
                    _ => false,
                });

                // Duplicate key — overwrite rid in place.
                if pos < keys.len() {
                    if let Value::Int(existing) = &keys[pos] {
                        if *existing == key {
                            values[pos] = rid;
                            return None;
                        }
                    }
                }

                keys.insert(pos, Value::Int(key));
                values.insert(pos, rid);

                if keys.len() <= ORDER {
                    return None;
                }

                // Overflow — split (same shape as `insert_recursive`).
                let mid = keys.len() / 2;
                let right_keys = keys.split_off(mid);
                let right_values = values.split_off(mid);
                let mid_key = match &right_keys[0] {
                    Value::Int(i) => *i,
                    _ => unreachable!("int-keyed btree held non-int key"),
                };
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
                // First child whose separator key is strictly greater than
                // `key`. Same single-sided i64 match as `lookup_int`.
                let pos = keys.partition_point(|k| match k {
                    Value::Int(i) => *i <= key,
                    _ => false,
                });
                let child_id = children[pos];
                // Drop the borrow on self.nodes[node_id] before recursing.

                let (mid_key, new_child_id) = self.insert_recursive_int(child_id, key, rid)?;

                // Re-borrow to insert the promoted key; possibly split.
                let split_payload = match &mut self.nodes[node_id] {
                    Node::Internal { keys, children } => {
                        keys.insert(pos, Value::Int(mid_key));
                        children.insert(pos + 1, new_child_id);
                        if keys.len() <= ORDER {
                            None
                        } else {
                            let mid = keys.len() / 2;
                            let promote_key = match &keys[mid] {
                                Value::Int(i) => *i,
                                _ => unreachable!("int-keyed internal held non-int key"),
                            };
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

                let (mid_key, new_child_id) = self.insert_recursive(child_id, key, rid)?;

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

    /// Mission C Phase 11: specialised int-keyed delete.
    ///
    /// Same rationale as `lookup_int`: the generic `delete` path runs every
    /// key comparison through `<Value as Ord>::cmp`, which matches on the
    /// discriminant of **both** sides before forwarding to `i64::cmp`. On
    /// `delete_by_filter` (~3300 rows per iteration × 3 iterations × ~12
    /// comparisons per descent = ~120K dispatch-heavy comparisons) that's a
    /// measurable fraction of the total. This fast path takes the key as a
    /// raw `i64` and uses single-sided discriminant matching so LLVM can
    /// compile the binary-search loop down to a straight `i64::cmp`.
    ///
    /// Returns `true` if the key was found and removed.
    #[inline]
    pub fn delete_int(&mut self, key: i64) -> bool {
        // Blocker B3: we mark dirty optimistically even if the key
        // turns out to be missing — the cost of re-checking is higher
        // than the cost of one no-op save.
        self.dirty = true;
        let mut node_id = self.root;
        loop {
            // Walk internal nodes via single-sided comparison.
            match &self.nodes[node_id] {
                Node::Internal { keys, children } => {
                    let pos = keys.partition_point(|k| match k {
                        Value::Int(i) => *i <= key,
                        _ => false,
                    });
                    node_id = children[pos];
                    continue;
                }
                Node::Leaf { .. } => {}
            }
            // Leaf: binary search then remove.
            if let Node::Leaf { keys, values, .. } = &mut self.nodes[node_id] {
                let result = keys.binary_search_by(|k| match k {
                    Value::Int(i) => i.cmp(&key),
                    _ => std::cmp::Ordering::Less,
                });
                if let Ok(pos) = result {
                    keys.remove(pos);
                    values.remove(pos);
                    return true;
                }
                return false;
            }
            unreachable!();
        }
    }

    /// Mission C Phase 12: batch-delete many int keys in a single tree walk.
    ///
    /// Given a **sorted ascending** list of int keys to remove, walks the
    /// leaf chain in order and compacts each affected leaf in a single pass.
    ///
    /// For a bulk delete of ~20% of rows, this replaces ~20K individual
    /// `Vec::remove` operations (each O(n) memmove of up to 4KB of `Value`
    /// entries) with a single compact per affected leaf (one pass of
    /// swap-and-truncate). On a 100K-row `delete_by_filter` bench that
    /// collapses ~80MB of pure memmove work down to ~3MB — the difference
    /// between losing to SQLite and winning.
    ///
    /// Keys not present in the tree are silently skipped. Returns the
    /// number of keys actually removed.
    ///
    /// Caller contract: `sorted_keys` must be sorted ascending. Duplicates
    /// are tolerated (the first removes, subsequent see nothing to remove).
    pub fn delete_many_int(&mut self, sorted_keys: &[i64]) -> usize {
        if sorted_keys.is_empty() {
            return 0;
        }
        // Blocker B3: mark dirty; non-empty input means we will try
        // at least one leaf compaction, which may or may not remove
        // anything — still cheaper than a counted "only on match" flip.
        self.dirty = true;

        // Walk to the leftmost leaf. From there we can follow `next_leaf`
        // to visit every leaf in order — matching the sorted-key cursor.
        let mut node_id = self.root;
        while let Node::Internal { children, .. } = &self.nodes[node_id] {
            node_id = children[0];
        }

        let mut total_removed = 0usize;
        let mut key_cursor = 0usize;
        let mut current = Some(node_id);

        while let Some(nid) = current {
            // Early exit: no more keys to delete.
            if key_cursor >= sorted_keys.len() {
                break;
            }

            let next_leaf = if let Node::Leaf { keys, values, next_leaf } = &mut self.nodes[nid] {
                let mut write = 0usize;
                for read in 0..keys.len() {
                    // Pull the int key out of the Value wrapper. Non-int
                    // keys shouldn't appear on an int index, but keep them
                    // defensively — they're impossible to match against an
                    // `i64` cursor anyway.
                    let k_opt = match &keys[read] {
                        Value::Int(i) => Some(*i),
                        _ => None,
                    };

                    // Advance cursor past any delete-keys smaller than the
                    // current leaf key. Those were either in a previous
                    // leaf or not present in the tree at all.
                    if let Some(k) = k_opt {
                        while key_cursor < sorted_keys.len()
                            && sorted_keys[key_cursor] < k
                        {
                            key_cursor += 1;
                        }
                        if key_cursor < sorted_keys.len() && sorted_keys[key_cursor] == k {
                            // Match — skip this entry from the output.
                            // Duplicates in sorted_keys still only drop one
                            // btree entry; advance cursor once, then let
                            // any further duplicates drop through to the
                            // "< k" advance on the next iteration.
                            key_cursor += 1;
                            total_removed += 1;
                            continue;
                        }
                    }

                    // Keep this entry. Move it down to the write index if
                    // we've already dropped anything.
                    if read != write {
                        keys.swap(read, write);
                        values.swap(read, write);
                    }
                    write += 1;
                }
                keys.truncate(write);
                values.truncate(write);
                *next_leaf
            } else {
                break;
            };

            current = next_leaf;
        }

        total_removed
    }

    /// Delete a key from the tree. Returns true if the key was found and removed.
    pub fn delete(&mut self, key: &Value) -> bool {
        // Blocker B3: mark dirty; see `delete_int` for the optimistic
        // marking rationale.
        self.dirty = true;
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
        while let Node::Internal { keys, children } = &self.nodes[node_id] {
            // Mission D1: binary search for child descent.
            let pos = keys.partition_point(|k| k <= start);
            node_id = children[pos];
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

    /// Range scan from `start` to the end of the tree (start <= key).
    pub fn range_from(&self, start: &Value) -> Vec<(Value, RowId)> {
        let mut node_id = self.root;
        while let Node::Internal { keys, children } = &self.nodes[node_id] {
            let pos = keys.partition_point(|k| k <= start);
            node_id = children[pos];
        }
        let start = start.clone();
        let mut results = Vec::new();
        let mut current = Some(node_id);
        while let Some(nid) = current {
            match &self.nodes[nid] {
                Node::Leaf { keys, values, next_leaf } => {
                    for (i, k) in keys.iter().enumerate() {
                        if k >= &start {
                            results.push((k.clone(), values[i]));
                        }
                    }
                    current = *next_leaf;
                }
                _ => break,
            }
        }
        results
    }

    /// Range scan from the beginning of the tree to `end` (key <= end).
    pub fn range_to(&self, end: &Value) -> Vec<(Value, RowId)> {
        // Find the leftmost leaf.
        let mut node_id = self.root;
        while let Node::Internal { children, .. } = &self.nodes[node_id] {
            node_id = children[0];
        }
        let end = end.clone();
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
                        results.push((k.clone(), values[i]));
                    }
                    if done { break; }
                    current = *next_leaf;
                }
                _ => break,
            }
        }
        results
    }

    /// Number of entries in the tree.
    pub fn len(&self) -> usize {
        let mut count = 0;
        let mut node_id = self.root;
        // Find leftmost leaf
        while let Node::Internal { children, .. } = &self.nodes[node_id] {
            node_id = children[0];
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

    /// Mission 3: the on-disk file this tree is backed by. Callers (Table
    /// lifecycle) use this to know where to write when they call
    /// `save`/`save_to_path`.
    pub fn file_path(&self) -> &Path {
        &self.path
    }

    /// Persist the tree to its backing file atomically. Writes to a
    /// sibling `.tmp` path then renames over the target, matching the
    /// catalog's persist strategy.
    pub fn save(&mut self) -> io::Result<()> {
        let path = self.path.clone();
        self.save_to(&path)?;
        self.dirty = false;
        Ok(())
    }

    /// Blocker B3: persist the tree only if it has been mutated since
    /// the last successful `save` / `save_if_dirty`. Wired into
    /// [`crate::catalog::Catalog::checkpoint`] and its `Drop` impl so
    /// the write cost is paid at most once per checkpoint instead of
    /// once per insert/update/delete on the hot path.
    pub fn save_if_dirty(&mut self) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        self.save()
    }

    /// Is there unflushed work in this tree? Exposed for tests + the
    /// `Table::rebuild_dirty_indexes_from_heap` recovery path.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Force the dirty flag. Used by crash-recovery paths that know
    /// the in-memory tree may lag the heap (e.g. after WAL replay).
    #[inline]
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Persist the tree to an arbitrary path. Primarily used by tests
    /// and by the create-index rebuild path, which wants to write the
    /// file at a location supplied by the caller. Does NOT update
    /// `self.path` or the dirty flag.
    pub fn save_to(&self, path: &Path) -> io::Result<()> {
        let mut buf: Vec<u8> = Vec::with_capacity(64 + 32 * self.nodes.len());
        buf.extend_from_slice(BTREE_MAGIC);
        buf.extend_from_slice(&BTREE_VERSION.to_le_bytes());
        buf.extend_from_slice(&(self.root as u32).to_le_bytes());
        buf.extend_from_slice(&(self.nodes.len() as u32).to_le_bytes());
        for node in &self.nodes {
            match node {
                Node::Internal { keys, children } => {
                    buf.push(NODE_TAG_INTERNAL);
                    buf.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                    for k in keys {
                        write_value(&mut buf, k);
                    }
                    buf.extend_from_slice(&(children.len() as u32).to_le_bytes());
                    for &c in children {
                        buf.extend_from_slice(&(c as u32).to_le_bytes());
                    }
                }
                Node::Leaf { keys, values, next_leaf } => {
                    buf.push(NODE_TAG_LEAF);
                    buf.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                    for k in keys {
                        write_value(&mut buf, k);
                    }
                    // values align 1:1 with keys, so we don't repeat the
                    // count — caller reads `n_keys` rids.
                    for rid in values {
                        buf.extend_from_slice(&rid.page_id.to_le_bytes());
                        buf.extend_from_slice(&rid.slot_index.to_le_bytes());
                    }
                    match next_leaf {
                        Some(nid) => {
                            buf.push(1);
                            buf.extend_from_slice(&(*nid as u32).to_le_bytes());
                        }
                        None => {
                            buf.push(0);
                        }
                    }
                }
            }
        }

        // Atomic write: sibling .tmp then rename. Mirrors the catalog's
        // persist strategy so a crash between write and rename leaves
        // the old file intact.
        let mut tmp = path.to_path_buf();
        let tmp_name = match path.file_name() {
            Some(n) => {
                let mut s = n.to_os_string();
                s.push(".tmp");
                s
            }
            None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "btree path has no file name")),
        };
        tmp.set_file_name(tmp_name);

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&buf)?;
        f.sync_data()?;
        drop(f);
        fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Reload a tree from disk. The returned tree's `path` is set to the
    /// supplied path so subsequent `save()` calls hit the same file.
    pub fn load(path: &Path) -> io::Result<Self> {
        let mut f = fs::File::open(path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        let mut pos = 0usize;
        if buf.len() < 14 || &buf[0..4] != BTREE_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad btree magic"));
        }
        pos += 4;
        let version = read_u16(&buf, &mut pos)?;
        if version != BTREE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported btree version: {version}"),
            ));
        }
        let root = read_u32(&buf, &mut pos)? as usize;
        let n_nodes = read_u32(&buf, &mut pos)? as usize;

        let mut nodes: Vec<Node> = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            let tag = read_u8(&buf, &mut pos)?;
            match tag {
                NODE_TAG_INTERNAL => {
                    let n_keys = read_u32(&buf, &mut pos)? as usize;
                    let mut keys = Vec::with_capacity(n_keys);
                    for _ in 0..n_keys {
                        keys.push(read_value(&buf, &mut pos)?);
                    }
                    let n_children = read_u32(&buf, &mut pos)? as usize;
                    let mut children = Vec::with_capacity(n_children);
                    for _ in 0..n_children {
                        children.push(read_u32(&buf, &mut pos)? as usize);
                    }
                    nodes.push(Node::Internal { keys, children });
                }
                NODE_TAG_LEAF => {
                    let n_keys = read_u32(&buf, &mut pos)? as usize;
                    let mut keys = Vec::with_capacity(n_keys);
                    for _ in 0..n_keys {
                        keys.push(read_value(&buf, &mut pos)?);
                    }
                    let mut values = Vec::with_capacity(n_keys);
                    for _ in 0..n_keys {
                        let page_id = read_u32(&buf, &mut pos)?;
                        let slot_index = read_u16(&buf, &mut pos)?;
                        values.push(RowId { page_id, slot_index });
                    }
                    let has_next = read_u8(&buf, &mut pos)? != 0;
                    let next_leaf = if has_next {
                        Some(read_u32(&buf, &mut pos)? as usize)
                    } else {
                        None
                    };
                    nodes.push(Node::Leaf { keys, values, next_leaf });
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown btree node tag: {other}"),
                    ));
                }
            }
        }

        if root >= nodes.len() && !nodes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "btree root index out of range",
            ));
        }

        Ok(BTree {
            nodes,
            root,
            path: path.to_path_buf(),
            // Loaded tree matches its backing file exactly.
            dirty: false,
        })
    }
}

// ─── on-disk value encoding ────────────────────────────────────────────────

fn write_value(buf: &mut Vec<u8>, v: &Value) {
    buf.push(v.type_id() as u8);
    match v {
        Value::Int(i) => buf.extend_from_slice(&i.to_le_bytes()),
        Value::Float(f) => buf.extend_from_slice(&f.to_le_bytes()),
        Value::Bool(b) => buf.push(if *b { 1 } else { 0 }),
        Value::Str(s) => {
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        Value::DateTime(t) => buf.extend_from_slice(&t.to_le_bytes()),
        Value::Uuid(u) => buf.extend_from_slice(u),
        Value::Bytes(b) => {
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
        Value::Empty => {}
    }
}

fn read_value(buf: &[u8], pos: &mut usize) -> io::Result<Value> {
    let tag = read_u8(buf, pos)?;
    let type_id = type_id_from_u8(tag)?;
    match type_id {
        TypeId::Int => {
            let v = read_i64(buf, pos)?;
            Ok(Value::Int(v))
        }
        TypeId::Float => {
            let raw = read_u64(buf, pos)?;
            Ok(Value::Float(f64::from_bits(raw)))
        }
        TypeId::Bool => {
            let b = read_u8(buf, pos)?;
            Ok(Value::Bool(b != 0))
        }
        TypeId::Str => {
            let n = read_u32(buf, pos)? as usize;
            if *pos + n > buf.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated btree str"));
            }
            let s = std::str::from_utf8(&buf[*pos..*pos + n])
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 in btree str"))?
                .to_string();
            *pos += n;
            Ok(Value::Str(s))
        }
        TypeId::DateTime => {
            let v = read_i64(buf, pos)?;
            Ok(Value::DateTime(v))
        }
        TypeId::Uuid => {
            if *pos + 16 > buf.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated btree uuid"));
            }
            let mut u = [0u8; 16];
            u.copy_from_slice(&buf[*pos..*pos + 16]);
            *pos += 16;
            Ok(Value::Uuid(u))
        }
        TypeId::Bytes => {
            let n = read_u32(buf, pos)? as usize;
            if *pos + n > buf.len() {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated btree bytes"));
            }
            let v = buf[*pos..*pos + n].to_vec();
            *pos += n;
            Ok(Value::Bytes(v))
        }
        TypeId::Empty => Ok(Value::Empty),
    }
}

fn type_id_from_u8(v: u8) -> io::Result<TypeId> {
    match v {
        0 => Ok(TypeId::Empty),
        1 => Ok(TypeId::Int),
        2 => Ok(TypeId::Float),
        3 => Ok(TypeId::Bool),
        4 => Ok(TypeId::Str),
        5 => Ok(TypeId::DateTime),
        6 => Ok(TypeId::Uuid),
        7 => Ok(TypeId::Bytes),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown btree value tag: {other}"),
        )),
    }
}

fn read_u8(buf: &[u8], pos: &mut usize) -> io::Result<u8> {
    if *pos >= buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated btree"));
    }
    let v = buf[*pos];
    *pos += 1;
    Ok(v)
}
fn read_u16(buf: &[u8], pos: &mut usize) -> io::Result<u16> {
    if *pos + 2 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated btree"));
    }
    let v = u16::from_le_bytes(buf[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}
fn read_u32(buf: &[u8], pos: &mut usize) -> io::Result<u32> {
    if *pos + 4 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated btree"));
    }
    let v = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}
fn read_u64(buf: &[u8], pos: &mut usize) -> io::Result<u64> {
    if *pos + 8 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated btree"));
    }
    let v = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}
fn read_i64(buf: &[u8], pos: &mut usize) -> io::Result<i64> {
    Ok(read_u64(buf, pos)? as i64)
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
    fn test_insert_int_matches_insert() {
        // Mission C Phase 15: specialized int insert path must produce a
        // tree indistinguishable from repeated generic `insert` calls
        // across the full key space, including updates (duplicate keys),
        // splits, and interleaved reads.
        let mut bt_fast = temp_btree("insert_int_fast");
        let mut bt_refn = temp_btree("insert_int_refn");
        for i in 0..5000i64 {
            let rid = RowId {
                page_id: (i / 256) as u32,
                slot_index: (i % 256) as u16,
            };
            bt_fast.insert_int(i, rid);
            bt_refn.insert(Value::Int(i), rid);
        }
        // Cross-check every key 0..5000 and a few missing ones on the
        // edges.
        for i in -5..5005 {
            assert_eq!(bt_fast.lookup_int(i), bt_refn.lookup_int(i), "divergence at key {i}");
        }
        assert_eq!(bt_fast.len(), bt_refn.len());

        // Duplicate-key update via the fast path should land on the same
        // slot as the generic insert.
        let new_rid = RowId { page_id: 999, slot_index: 42 };
        bt_fast.insert_int(100, new_rid);
        bt_refn.insert(Value::Int(100), new_rid);
        assert_eq!(bt_fast.lookup_int(100), Some(new_rid));
        assert_eq!(bt_refn.lookup_int(100), Some(new_rid));
        assert_eq!(bt_fast.len(), bt_refn.len());
    }

    #[test]
    fn test_insert_int_reverse_order_splits() {
        // Exercise descending-key insertion, which stresses the leaf
        // split path because every insert lands at position 0.
        let mut bt = temp_btree("insert_int_reverse");
        for i in (0..1000i64).rev() {
            bt.insert_int(i, RowId { page_id: 0, slot_index: i as u16 });
        }
        for i in 0..1000i64 {
            assert_eq!(
                bt.lookup_int(i),
                Some(RowId { page_id: 0, slot_index: i as u16 }),
                "missing key {i}",
            );
        }
        assert_eq!(bt.len(), 1000);
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
    fn test_delete_many_int_matches_per_key_delete() {
        // Mission C Phase 12: batch delete must agree with repeated
        // per-key `delete_int` calls on every lookup across the full tree.
        let mut bt_batch = temp_btree("delete_many_batch");
        let mut bt_refn = temp_btree("delete_many_refn");
        for i in 0..5000i64 {
            let rid = RowId {
                page_id: (i / 256) as u32,
                slot_index: (i % 256) as u16,
            };
            bt_batch.insert(Value::Int(i), rid);
            bt_refn.insert(Value::Int(i), rid);
        }

        // Delete every 3rd key, plus some missing keys for good measure.
        let mut to_delete: Vec<i64> = (0..5000).filter(|i| i % 3 == 0).collect();
        // Inject missing keys that shouldn't affect anything.
        to_delete.push(7000);
        to_delete.push(-5);
        to_delete.sort();

        let removed = bt_batch.delete_many_int(&to_delete);
        // 5000 / 3 rounded up = 1667 present keys deleted; the two missing
        // keys should be silently skipped.
        let expected_removed = to_delete.iter().filter(|k| **k >= 0 && **k < 5000).count();
        assert_eq!(removed, expected_removed);

        for k in &to_delete {
            bt_refn.delete_int(*k);
        }

        // Cross-check every key 0..5000 (present or absent).
        for i in 0..5000i64 {
            let a = bt_batch.lookup_int(i);
            let b = bt_refn.lookup_int(i);
            assert_eq!(a, b, "divergence at key {i}");
        }
        assert_eq!(bt_batch.len(), bt_refn.len());
    }

    #[test]
    fn test_delete_many_int_empty_slice() {
        let mut bt = temp_btree("delete_many_empty");
        for i in 0..100 {
            bt.insert(Value::Int(i), RowId { page_id: 0, slot_index: i as u16 });
        }
        let removed = bt.delete_many_int(&[]);
        assert_eq!(removed, 0);
        assert_eq!(bt.len(), 100);
    }

    #[test]
    fn test_delete_many_int_all_missing() {
        let mut bt = temp_btree("delete_many_missing");
        for i in 0..100 {
            bt.insert(Value::Int(i), RowId { page_id: 0, slot_index: i as u16 });
        }
        let removed = bt.delete_many_int(&[1000, 2000, 3000]);
        assert_eq!(removed, 0);
        assert_eq!(bt.len(), 100);
    }

    #[test]
    fn test_delete_many_int_all_keys() {
        let mut bt = temp_btree("delete_many_all");
        let keys: Vec<i64> = (0..500).collect();
        for &i in &keys {
            bt.insert(Value::Int(i), RowId { page_id: 0, slot_index: i as u16 });
        }
        let removed = bt.delete_many_int(&keys);
        assert_eq!(removed, 500);
        assert_eq!(bt.len(), 0);
        for i in &keys {
            assert_eq!(bt.lookup_int(*i), None);
        }
    }

    #[test]
    fn test_save_load_roundtrip_int_keys() {
        // Mission 3: save + load must reproduce an equivalent tree for
        // both small (single-leaf) and large (multi-level) int-keyed
        // trees. "Equivalent" means every key looks up to the same
        // RowId, and the total length matches.
        let tmp = std::env::temp_dir().join(format!(
            "powdb_btree_save_int_{}.idx",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);

        let mut bt = BTree::create(&tmp).unwrap();
        for i in 0..2000i64 {
            bt.insert_int(
                i,
                RowId { page_id: (i / 256) as u32, slot_index: (i % 256) as u16 },
            );
        }
        bt.save().unwrap();

        let reloaded = BTree::load(&tmp).unwrap();
        assert_eq!(reloaded.len(), bt.len());
        for i in 0..2000i64 {
            let orig = bt.lookup_int(i);
            let round = reloaded.lookup_int(i);
            assert_eq!(orig, round, "mismatch at key {i}");
            assert!(round.is_some());
        }
        // Missing keys stay missing.
        assert_eq!(reloaded.lookup_int(-1), None);
        assert_eq!(reloaded.lookup_int(9999), None);
        // Range scan across splits should match order.
        let orig_range: Vec<_> = bt.range(&Value::Int(500), &Value::Int(600)).collect();
        let round_range: Vec<_> =
            reloaded.range(&Value::Int(500), &Value::Int(600)).collect();
        assert_eq!(orig_range, round_range);

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_save_load_roundtrip_mixed_value_types() {
        // String keys exercise the variable-length value encoder path.
        let tmp = std::env::temp_dir().join(format!(
            "powdb_btree_save_mixed_{}.idx",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);

        let mut bt = BTree::create(&tmp).unwrap();
        let keys = [
            ("alice", RowId { page_id: 1, slot_index: 0 }),
            ("bob", RowId { page_id: 2, slot_index: 1 }),
            ("charlie", RowId { page_id: 3, slot_index: 2 }),
            ("diana", RowId { page_id: 4, slot_index: 3 }),
            ("", RowId { page_id: 5, slot_index: 4 }),
            ("unicode ⚡", RowId { page_id: 6, slot_index: 5 }),
        ];
        for (k, rid) in keys.iter() {
            bt.insert(Value::Str((*k).into()), *rid);
        }
        bt.save().unwrap();

        let reloaded = BTree::load(&tmp).unwrap();
        assert_eq!(reloaded.len(), keys.len());
        for (k, expected_rid) in keys.iter() {
            let got = reloaded.lookup(&Value::Str((*k).into()));
            assert_eq!(got, Some(*expected_rid), "mismatch at key {k:?}");
        }
        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_save_load_empty_tree() {
        // A freshly created empty tree must round-trip (one empty leaf,
        // root = 0).
        let tmp = std::env::temp_dir().join(format!(
            "powdb_btree_save_empty_{}.idx",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&tmp);
        let mut bt = BTree::create(&tmp).unwrap();
        bt.save().unwrap();
        let reloaded = BTree::load(&tmp).unwrap();
        assert_eq!(reloaded.len(), 0);
        assert!(reloaded.is_empty());
        assert_eq!(reloaded.lookup(&Value::Int(42)), None);
        std::fs::remove_file(&tmp).ok();
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
