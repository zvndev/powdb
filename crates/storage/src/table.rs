use crate::btree::BTree;
use crate::heap::HeapFile;
use crate::row::{encode_row, decode_row};
use crate::types::*;
use std::collections::HashMap;
use std::io;
use std::path::Path;

/// A table combines a heap file, schema, and optional indexes.
pub struct Table {
    pub schema: Schema,
    pub heap: HeapFile,
    pub indexes: HashMap<String, BTree>, // column_name -> index
}

impl Table {
    pub fn create(schema: Schema, data_dir: &Path) -> io::Result<Self> {
        let heap_path = data_dir.join(format!("{}.heap", schema.table_name));
        let heap = HeapFile::create(&heap_path)?;
        Ok(Table { schema, heap, indexes: HashMap::new() })
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
        self.heap.delete(rid);
        Ok(())
    }

    pub fn update(&mut self, rid: RowId, values: &Row) -> io::Result<RowId> {
        self.delete(rid)?;
        self.insert(values)
    }

    pub fn scan(&self) -> impl Iterator<Item = (RowId, Row)> + '_ {
        self.heap.scan().map(|(rid, data)| {
            (rid, decode_row(&self.schema, &data))
        })
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
