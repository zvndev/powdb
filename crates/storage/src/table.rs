use crate::btree::BTree;
use crate::heap::HeapFile;
use crate::row::{decode_column, decode_row, encode_row_into, patch_var_column_in_place, RowLayout};
use crate::types::*;
use std::io;
use std::path::Path;

/// Per-indexed-column metadata owning the BTree inline.
///
/// Mission C Phase 15 introduced this struct as a cache of `col_idx`,
/// `col_name`, and `is_int` so the hot `Table::insert` path could skip
/// the schema column-name linear scan. Mission C Phase 17 folds the
/// BTree itself into this struct, retiring the parallel
/// `FxHashMap<String, BTree>` that the hot write paths were otherwise
/// forced to probe every single call. Everything the write paths need
/// is now in a single tight `Vec<IndexedCol>` — no hash, no string
/// compare, no out-of-line allocation.
pub(crate) struct IndexedCol {
    /// Schema column index of the indexed column.
    pub col_idx: usize,
    /// Column name — still needed to resolve name-based lookups from the
    /// executor (`tbl.index("id")`, etc.). Cost is only paid on the
    /// rarer name-keyed read paths.
    pub col_name: String,
    /// `true` when the column type is `TypeId::Int`. Lets `insert` /
    /// `delete` take the `insert_int` / `delete_int` fast paths without
    /// re-matching the schema every call.
    pub is_int: bool,
    /// The B+ tree. Lives inline alongside the metadata so the hot
    /// insert/delete/update loops can touch a single cache line per
    /// index entry instead of chasing a separate HashMap probe.
    pub btree: BTree,
}

/// A table combines a heap file, schema, and optional indexes.
///
/// Mission C Phase 17: indexes used to live in a `FxHashMap<String,
/// BTree>` alongside a parallel `Vec<IndexedCol>` of metadata. Every row
/// insert paid an FxHash of the index column name to look the btree back
/// out of the map. This phase collapses both data structures into a
/// single `Vec<IndexedCol>` where each entry owns its btree inline —
/// the hot write path walks one small vec and calls straight through to
/// `insert_int`.
///
/// Mission C Phase 2: holds `encode_scratch`, a reusable buffer for
/// [`crate::row::encode_row_into`]. Bench loops that push thousands of
/// rows through `insert`/`update` reuse the same allocation across calls,
/// cutting the allocator traffic to ~zero after the first row.
pub struct Table {
    pub schema: Schema,
    pub heap: HeapFile,
    /// Reusable scratch buffer for row encoding. Cleared on every call.
    encode_scratch: Vec<u8>,
    /// Per-indexed-column metadata, each entry owning its BTree inline.
    /// Public to the crate so the query executor's IndexScan fast paths
    /// can reach in via the `index()` / `index_mut()` helpers instead
    /// of probing a separate hash map.
    pub(crate) indexed_cols: Vec<IndexedCol>,
    /// Mission C Phase 7: cached row layout so `delete` can decode only
    /// the indexed columns out of the raw page bytes without running the
    /// full per-row offset calculation every call.
    row_layout: RowLayout,
}

impl Table {
    pub fn create(schema: Schema, data_dir: &Path) -> io::Result<Self> {
        let heap_path = data_dir.join(format!("{}.heap", schema.table_name));
        let heap = HeapFile::create(&heap_path)?;
        let row_layout = RowLayout::new(&schema);
        Ok(Table {
            schema,
            heap,
            encode_scratch: Vec::new(),
            indexed_cols: Vec::new(),
            row_layout,
        })
    }

    /// Reopen an existing table from disk. Caller supplies the schema (loaded
    /// from the catalog file). Indexes are not rebuilt — they live in memory
    /// until `create_index` is called again.
    pub fn open(schema: Schema, data_dir: &Path) -> io::Result<Self> {
        let heap_path = data_dir.join(format!("{}.heap", schema.table_name));
        let heap = HeapFile::open(&heap_path)?;
        let row_layout = RowLayout::new(&schema);
        Ok(Table {
            schema,
            heap,
            encode_scratch: Vec::new(),
            indexed_cols: Vec::new(),
            row_layout,
        })
    }

    /// Look up an index by column name. Returns `None` if no index on
    /// this column. Used by the read-side executor paths (IndexScan,
    /// Project(IndexScan), etc.) that still need name-based resolution;
    /// the write-side hot paths iterate `indexed_cols` directly.
    #[inline]
    pub fn index(&self, col_name: &str) -> Option<&BTree> {
        self.indexed_cols
            .iter()
            .find(|c| c.col_name == col_name)
            .map(|c| &c.btree)
    }

    /// Mutable counterpart to [`Self::index`].
    #[inline]
    pub fn index_mut(&mut self, col_name: &str) -> Option<&mut BTree> {
        self.indexed_cols
            .iter_mut()
            .find(|c| c.col_name == col_name)
            .map(|c| &mut c.btree)
    }

    /// `true` if this table has an index on the named column.
    #[inline]
    pub fn has_index(&self, col_name: &str) -> bool {
        self.indexed_cols.iter().any(|c| c.col_name == col_name)
    }

    /// `true` if this table has no secondary indexes at all.
    #[inline]
    pub fn indexes_is_empty(&self) -> bool {
        self.indexed_cols.is_empty()
    }

    /// Mission C Phase 15: the hot insert path used to do two wasted
    /// things per secondary index, on every row:
    ///   1. `for (col_name, btree) in &mut self.indexes` walked an
    ///      FxHashMap by iterator (cheap but not free), and
    ///   2. `self.schema.column_index(col_name)` walked `schema.columns`
    ///      doing an O(n_cols) strcmp linear search to translate the
    ///      column name back into its schema position.
    ///
    /// For the `insert_batch_1k` bench (1K rows, User table, one index on
    /// `id`) that came out to ~6 strcmps * 1000 rows = 6K wasted
    /// comparisons per iteration, plus the HashMap iter overhead. We now
    /// iterate the precomputed `indexed_cols` slice directly, which hands
    /// us `(col_idx, col_name, is_int)` per entry, and route int keys
    /// straight through `BTree::insert_int` to skip the generic
    /// `Value::Ord` dispatch on every binary-search comparison.
    pub fn insert(&mut self, values: &Row) -> io::Result<RowId> {
        encode_row_into(&self.schema, values, &mut self.encode_scratch);
        let rid = self.heap.insert(&self.encode_scratch)?;

        // Fast path: no indexes — skip the whole loop entirely.
        if self.indexed_cols.is_empty() {
            return Ok(rid);
        }

        // Mission C Phase 17: the btree lives inline in IndexedCol now,
        // so this loop does zero hash lookups. For a 1-index table
        // (bench's `User.id` case) the body compiles down to one
        // bounds-checked vec access + one `insert_int` call, no
        // FxHash(col_name) / HashMap probe at all.
        for entry in &mut self.indexed_cols {
            let val = &values[entry.col_idx];
            if val.is_empty() {
                continue;
            }
            if entry.is_int {
                if let Value::Int(i) = val {
                    entry.btree.insert_int(*i, rid);
                    continue;
                }
            }
            entry.btree.insert(val.clone(), rid);
        }
        Ok(rid)
    }

    pub fn get(&self, rid: RowId) -> Option<Row> {
        let data = self.heap.get(rid)?;
        Some(decode_row(&self.schema, &data))
    }

    /// Delete a row. Mission C Phase 7: if the table has indexes, we used to
    /// call `decode_row` here — allocating `Row` + every column's `Value`
    /// just to read the two or three columns that actually feed the index.
    /// Now we borrow the raw page bytes once and call `decode_column` for
    /// exactly the indexed columns, skipping the rest of the row entirely.
    ///
    /// Mission C Phase 11: the Phase 7 version still allocated a
    /// `Vec<(usize, Value)>` per row so the btree mutations could happen
    /// after the hot-page borrow closed. That's 3300 heap allocations per
    /// 100K-row `delete_by_filter` iteration — gone in Phase 11 via
    /// struct-field borrow splitting, so the btree lives alongside the
    /// page borrow inside the closure.
    pub fn delete(&mut self, rid: RowId) -> io::Result<()> {
        if self.indexed_cols.is_empty() {
            return self.heap.delete(rid);
        }

        // Split the borrow so `indexed_cols` (mutable — the btree lives
        // inside each entry now) can be captured by the closure alongside
        // `heap` (also mutable). Rust's disjoint-field borrowing lets
        // this compile without cloning anything.
        let Table {
            heap,
            schema,
            row_layout: layout,
            indexed_cols,
            ..
        } = self;

        heap.with_row_bytes(rid, |data| {
            for entry in indexed_cols.iter_mut() {
                let val = decode_column(schema, layout, data, entry.col_idx);
                if val.is_empty() {
                    continue;
                }
                // Mission C Phase 11: dispatch to delete_int when the
                // indexed key is an integer — skips Value::Ord dispatch
                // in the btree binary search and partition_point walk.
                match &val {
                    Value::Int(i) => {
                        entry.btree.delete_int(*i);
                    }
                    _ => {
                        entry.btree.delete(&val);
                    }
                }
            }
        })?;

        self.heap.delete(rid)
    }

    /// Mission C Phase 12: bulk delete a list of rids, batching the
    /// secondary-index maintenance.
    ///
    /// For a 100K-row `delete_by_filter` that removes ~20% of the rows,
    /// the per-row `Table::delete` path pays ~4ms of pure `Vec::remove`
    /// memmove inside the btree: every call shifts up to 4KB of leaf
    /// entries. This helper collects the indexed-column keys first,
    /// deletes the heap slots one by one (hot-page writes), then compacts
    /// each btree in a single pass via [`BTree::delete_many_int`].
    ///
    /// Restrictions / fall-through:
    /// - If the table has no indexes, this is equivalent to looping over
    ///   `heap.delete`.
    /// - If any indexed column is not `TypeId::Int`, this falls back to
    ///   the per-row `delete` path. The int-only constraint matches the
    ///   only btree batch primitive we have (`delete_many_int`) and
    ///   covers the overwhelmingly common case (primary keys,
    ///   `created_at`, foreign keys).
    ///
    /// Returns the number of rows removed.
    pub fn delete_many(&mut self, rids: &[RowId]) -> io::Result<u64> {
        if rids.is_empty() {
            return Ok(0);
        }
        if self.indexed_cols.is_empty() {
            for &rid in rids {
                self.heap.delete(rid)?;
            }
            return Ok(rids.len() as u64);
        }

        // All indexed cols must be int for the batch btree path to apply.
        // Phase 15: `is_int` is precomputed at create_index time, so this
        // is now a straight bool AND across the slice.
        let all_int = self.indexed_cols.iter().all(|c| c.is_int);
        if !all_int {
            // Mixed index types — defer to the generic per-row path.
            let mut count = 0u64;
            for &rid in rids {
                self.delete(rid)?;
                count += 1;
            }
            return Ok(count);
        }

        // Split the borrow so the closure can capture `schema`/`layout`/
        // `indexed_cols` while `heap` is borrowed mutably by
        // `delete_with_hook`.
        let Table {
            heap,
            schema,
            row_layout: layout,
            indexed_cols,
            ..
        } = self;

        let n_indexed = indexed_cols.len();
        let mut keys_per_index: Vec<Vec<i64>> = (0..n_indexed)
            .map(|_| Vec::with_capacity(rids.len()))
            .collect();

        let mut count = 0u64;
        for &rid in rids {
            let found = heap.delete_with_hook(rid, |data| {
                for (slot_i, entry) in indexed_cols.iter().enumerate() {
                    if let Value::Int(i) = decode_column(schema, layout, data, entry.col_idx) {
                        keys_per_index[slot_i].push(i);
                    }
                }
            })?;
            if found {
                count += 1;
            }
        }

        // Batch-compact each btree in a single leaf-chain walk. Mission C
        // Phase 17: btrees now live inline in indexed_cols, so this is a
        // direct `iter_mut()` over the same slice the hook above borrowed
        // immutably — no HashMap probe required.
        for (slot_i, entry) in indexed_cols.iter_mut().enumerate() {
            let keys = &mut keys_per_index[slot_i];
            keys.sort_unstable();
            entry.btree.delete_many_int(keys);
        }

        Ok(count)
    }

    /// Single-pass scan-and-delete driven by a raw-bytes predicate. Walks
    /// the heap once, marks matching rows deleted in place, and updates
    /// any int-keyed secondary indexes in a single batched
    /// `delete_many_int` per index at the end. Non-int secondary indexes
    /// fall back to per-key `btree.delete`, but still ride the same
    /// single heap pass.
    ///
    /// Mission C Phase 16: this is the Table-level hook for
    /// [`HeapFile::scan_delete_matching`]. See that method for the
    /// fusion rationale. The executor's `Delete` fast path routes
    /// `Filter(SeqScan)` / `SeqScan`-shaped delete plans here when the
    /// predicate compiles.
    pub fn scan_delete_matching<P>(&mut self, pred: P) -> io::Result<u64>
    where
        P: FnMut(&[u8]) -> bool,
    {
        if self.indexed_cols.is_empty() {
            return self.heap.scan_delete_matching(pred, |_| {});
        }

        // Split the borrow so the hook closure can capture schema /
        // layout / indexed_cols (immutably for reads) while `heap` is
        // mutably borrowed by `scan_delete_matching`. After the scan
        // completes, the closure is dropped, freeing the shared borrow
        // of `indexed_cols` so we can flip to `iter_mut()` for the
        // batch btree compaction.
        let Table {
            heap,
            schema,
            row_layout: layout,
            indexed_cols,
            ..
        } = self;

        let n_indexed = indexed_cols.len();
        let all_int = indexed_cols.iter().all(|c| c.is_int);

        if all_int {
            let mut keys_per_index: Vec<Vec<i64>> =
                (0..n_indexed).map(|_| Vec::with_capacity(1024)).collect();

            let count = heap.scan_delete_matching(pred, |data| {
                for (slot_i, entry) in indexed_cols.iter().enumerate() {
                    if let Value::Int(i) = decode_column(schema, layout, data, entry.col_idx) {
                        keys_per_index[slot_i].push(i);
                    }
                }
            })?;

            // Mission C Phase 17: btrees live inline in indexed_cols,
            // so this direct iter_mut replaces the old HashMap probe.
            for (slot_i, entry) in indexed_cols.iter_mut().enumerate() {
                let keys = &mut keys_per_index[slot_i];
                keys.sort_unstable();
                entry.btree.delete_many_int(keys);
            }
            return Ok(count);
        }

        // Mixed / non-int secondary indexes: still do the single heap
        // pass, but fall back to per-key btree deletes at the end.
        let mut values_per_index: Vec<Vec<Value>> =
            (0..n_indexed).map(|_| Vec::with_capacity(256)).collect();

        let count = heap.scan_delete_matching(pred, |data| {
            for (slot_i, entry) in indexed_cols.iter().enumerate() {
                let v = decode_column(schema, layout, data, entry.col_idx);
                if !v.is_empty() {
                    values_per_index[slot_i].push(v);
                }
            }
        })?;

        for (slot_i, entry) in indexed_cols.iter_mut().enumerate() {
            for v in &values_per_index[slot_i] {
                entry.btree.delete(v);
            }
        }
        Ok(count)
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
        self.update_hinted(rid, values, None)
    }

    /// Same as `update`, but the caller can supply the set of column
    /// indices that actually changed. If supplied, the old-row read is
    /// skipped entirely when none of the changed columns is indexed.
    ///
    /// Mission C Phase 2: `update_by_filter` hits this path ~50K times with
    /// a single-column assignment (status) on a table whose only index is
    /// on `id`. The old code called `self.get(rid)` unconditionally — a
    /// heap read + full decode every time — even though the result was
    /// always thrown away for non-indexed updates. Skipping that read is
    /// worth ~300ns/row, or ~15ms on a 50K-row update_by_filter.
    pub fn update_hinted(
        &mut self,
        rid: RowId,
        values: &Row,
        changed_col_indices: Option<&[usize]>,
    ) -> io::Result<RowId> {
        let touches_index = if self.indexed_cols.is_empty() {
            false
        } else if let Some(changed) = changed_col_indices {
            self.indexed_cols
                .iter()
                .any(|c| changed.contains(&c.col_idx))
        } else {
            // No hint — fall back to the safe path that reads the old row.
            true
        };

        let old_row = if touches_index { self.get(rid) } else { None };

        encode_row_into(&self.schema, values, &mut self.encode_scratch);
        let new_rid = self.heap.update(rid, &self.encode_scratch)?;

        if touches_index {
            // Mission C Phase 17: walk the Vec<IndexedCol> directly.
            // `col_idx` is already precomputed on each entry, so we
            // don't even re-probe schema.column_index here.
            for entry in self.indexed_cols.iter_mut() {
                let new_val = &values[entry.col_idx];
                let old_val_opt = old_row.as_ref().map(|r| &r[entry.col_idx]);

                if let Some(old_val) = old_val_opt {
                    if old_val == new_val && new_rid == rid {
                        continue;
                    }
                    if !old_val.is_empty() {
                        entry.btree.delete(old_val);
                    }
                }
                if !new_val.is_empty() {
                    entry.btree.insert(new_val.clone(), new_rid);
                }
            }
        }
        Ok(new_rid)
    }

    /// Patch a row's raw bytes in place. Caller guarantees the mutation
    /// does not change the row's total length and does not touch any
    /// indexed column — indexes are NOT updated by this path.
    ///
    /// Mission C Phase 4: see `HeapFile::with_row_bytes_mut`. This is the
    /// primitive that backs the executor's single-column fixed-width
    /// update fast path.
    #[inline]
    pub fn with_row_bytes_mut<F>(&mut self, rid: RowId, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut [u8]),
    {
        self.heap.with_row_bytes_mut(rid, f)
    }

    /// Patch a single var-length column in place, shrinking the row when
    /// the new value is smaller than the old one. Returns `Ok(true)` on
    /// success, `Ok(false)` when the new value would grow the row or the
    /// slot is gone (caller should fall back to the full update path).
    ///
    /// The caller is responsible for ensuring no indexed column is
    /// touched by this patch — indexes are NOT maintained here.
    ///
    /// Mission C Phase 10: backs the executor's `update_by_filter` fast
    /// path for var-length single-column assignments.
    #[inline]
    pub fn patch_var_col_in_place(
        &mut self,
        rid: RowId,
        col_idx: usize,
        new_value: Option<&[u8]>,
    ) -> io::Result<bool> {
        let layout = &self.row_layout;
        self.heap.patch_row_shrink(rid, |bytes| {
            patch_var_column_in_place(bytes, layout, col_idx, new_value)
        })
    }

    /// Cached row layout for this table. Used by the executor to plan
    /// the byte-patch fast paths without re-walking the schema.
    #[inline]
    pub fn row_layout(&self) -> &RowLayout {
        &self.row_layout
    }

    /// Mission C Phase 15: does the given schema column index have an
    /// index attached? Used by the executor's update fast-path planner
    /// to decide whether a byte-patch update is safe (no index to
    /// maintain). Linear scan over `indexed_cols` — typically 1–3
    /// entries, so cheaper than a HashMap lookup by name.
    #[inline]
    pub fn has_indexed_col(&self, col_idx: usize) -> bool {
        self.indexed_cols.iter().any(|c| c.col_idx == col_idx)
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
        let btree = self.index(col_name)?;
        let rid = btree.lookup(key)?;
        let row = self.get(rid)?;
        Some((rid, row))
    }

    pub fn create_index(&mut self, col_name: &str, data_dir: &Path) -> io::Result<()> {
        let col_idx = self.schema.column_index(col_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "column not found"))?;

        // Mission C Phase 17: if this column already has an index,
        // no-op (matches the prior map.insert semantics of silently
        // replacing a duplicate, minus the wasted work).
        if self.indexed_cols.iter().any(|c| c.col_idx == col_idx) {
            return Ok(());
        }

        let idx_path = data_dir.join(format!("{}_{}.idx", self.schema.table_name, col_name));
        let mut btree = BTree::create(&idx_path)?;

        // Build index from existing data
        for (rid, row) in self.scan() {
            if !row[col_idx].is_empty() {
                btree.insert(row[col_idx].clone(), rid);
            }
        }

        // Mission C Phase 17: store the btree inline alongside the
        // cached col_idx / col_name / is_int metadata — single tight
        // entry per index, walked directly by the hot write paths.
        let is_int = self.schema.columns[col_idx].type_id == TypeId::Int;
        self.indexed_cols.push(IndexedCol {
            col_idx,
            col_name: col_name.to_string(),
            is_int,
            btree,
        });
        Ok(())
    }
}
