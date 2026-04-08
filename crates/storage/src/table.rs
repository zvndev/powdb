use crate::btree::BTree;
use crate::heap::HeapFile;
use crate::row::{encode_row, decode_row};
use crate::types::*;
use rustc_hash::FxHashMap;
use std::io;
use std::path::Path;

/// A table combines a heap file, schema, and optional indexes.
///
/// Mission F: indexes use FxHashMap. Per-row index lookup happens inside
/// every insert/delete/update — even one HashMap probe per row matters at
/// 200ns/op tier.
pub struct Table {
    pub schema: Schema,
    pub heap: HeapFile,
    pub indexes: FxHashMap<String, BTree>, // column_name -> index
}

impl Table {
    pub fn create(schema: Schema, data_dir: &Path) -> io::Result<Self> {
        let heap_path = data_dir.join(format!("{}.heap", schema.table_name));
        let heap = HeapFile::create(&heap_path)?;
        Ok(Table { schema, heap, indexes: FxHashMap::default() })
    }

    /// Reopen an existing table from disk. Caller supplies the schema (loaded
    /// from the catalog file). Indexes are not rebuilt — they live in memory
    /// until `create_index` is called again.
    pub fn open(schema: Schema, data_dir: &Path) -> io::Result<Self> {
        let heap_path = data_dir.join(format!("{}.heap", schema.table_name));
        let heap = HeapFile::open(&heap_path)?;
        Ok(Table { schema, heap, indexes: FxHashMap::default() })
    }

    pub fn insert(&mut self, values: &Row) -> io::Result<RowId> {
        let encoded = encode_row(&self.schema, values);
        let rid = self.heap.insert(&encoded)?;

        // Update all indexes
        for (col_name, btree) in &mut self.indexes {
            if let Some(idx) = self.schema.column_index(col_name) {
                if !values[idx].is_empty() {
                    btree.insert(values[idx].clone(), rid);
                }
            }
        }
        Ok(rid)
    }

    pub fn get(&self, rid: RowId) -> Option<Row> {
        let data = self.heap.get(rid)?;
        Some(decode_row(&self.schema, &data))
    }

    pub fn delete(&mut self, rid: RowId) -> io::Result<()> {
        // Remove from indexes
        if let Some(data) = self.heap.get(rid) {
            let row = decode_row(&self.schema, &data);
            for (col_name, btree) in &mut self.indexes {
                if let Some(idx) = self.schema.column_index(col_name) {
                    if !row[idx].is_empty() {
                        btree.delete(&row[idx]);
                    }
                }
            }
        }
        self.heap.delete(rid)
    }

    /// Update a row in place when possible. Falls back to delete+insert only
    /// if the new encoding doesn't fit in the current slot.
    ///
    /// Mission D5: the previous implementation always did `delete + insert`,
    /// which:
    ///   1. read+wrote the page twice (once to clear the slot, once to fill it
    ///      again — usually on a different page),
    ///   2. did an O(N) scan over `pages_with_space` for every insert,
    ///   3. mutated every index even when the indexed column hadn't changed.
    /// On `update_by_filter` (50K matching rows, status-only update, no
    /// index on status) that turned ~1ms of work into 30 seconds — a
    /// catastrophic O(N²)-ish gap vs SQLite (6.7ms total). The fix is to
    /// (a) prefer `heap.update` which tries in-place first and (b) only
    /// touch indexes whose value actually changed.
    pub fn update(&mut self, rid: RowId, values: &Row) -> io::Result<RowId> {
        // Read the old row once so we can both check which indexed columns
        // changed AND avoid touching indexes for unchanged columns.
        let old_row = self.get(rid);

        let encoded = encode_row(&self.schema, values);
        let new_rid = self.heap.update(rid, &encoded)?;

        // Index maintenance — only re-key columns whose value differs.
        // For the common "update one non-indexed column" case this is a
        // no-op walk over a tiny FxHashMap.
        if !self.indexes.is_empty() {
            for (col_name, btree) in &mut self.indexes {
                let Some(idx) = self.schema.column_index(col_name) else { continue };
                let new_val = &values[idx];
                let old_val_opt = old_row.as_ref().map(|r| &r[idx]);

                // Skip if column didn't change AND row didn't move.
                if let Some(old_val) = old_val_opt {
                    if old_val == new_val && new_rid == rid {
                        continue;
                    }
                    // Remove old entry — even if rid is unchanged, the key may differ.
                    if !old_val.is_empty() {
                        btree.delete(old_val);
                    }
                }
                if !new_val.is_empty() {
                    btree.insert(new_val.clone(), new_rid);
                }
            }
        }
        Ok(new_rid)
    }

    pub fn scan(&self) -> impl Iterator<Item = (RowId, Row)> + '_ {
        self.heap.scan().map(|(rid, data)| {
            (rid, decode_row(&self.schema, &data))
        })
    }

    /// Zero-copy scan that passes raw row bytes to the callback without
    /// decoding or allocating per-row. The caller is responsible for
    /// decoding only the columns it needs via `decode_column`.
    pub fn for_each_row_raw<F>(&self, f: F)
    where
        F: FnMut(RowId, &[u8]),
    {
        self.heap.for_each_row(f);
    }

    /// Zero-copy scan with early termination. The callback returns
    /// `ControlFlow::Break(())` to stop. Used by `Limit` fast paths.
    pub fn try_for_each_row_raw<F>(&self, f: F)
    where
        F: FnMut(RowId, &[u8]) -> std::ops::ControlFlow<()>,
    {
        self.heap.try_for_each_row(f);
    }

    pub fn index_lookup(&self, col_name: &str, key: &Value) -> Option<(RowId, Row)> {
        let btree = self.indexes.get(col_name)?;
        let rid = btree.lookup(key)?;
        let row = self.get(rid)?;
        Some((rid, row))
    }

    pub fn create_index(&mut self, col_name: &str, data_dir: &Path) -> io::Result<()> {
        let idx_path = data_dir.join(format!("{}_{}.idx", self.schema.table_name, col_name));
        let mut btree = BTree::create(&idx_path)?;

        // Build index from existing data
        let col_idx = self.schema.column_index(col_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "column not found"))?;
        for (rid, row) in self.scan() {
            if !row[col_idx].is_empty() {
                btree.insert(row[col_idx].clone(), rid);
            }
        }

        self.indexes.insert(col_name.to_string(), btree);
        Ok(())
    }
}
