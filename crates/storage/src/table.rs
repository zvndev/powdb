use crate::btree::BTree;
use crate::heap::HeapFile;
use crate::row::{
    decode_column, decode_row, encode_row_into_with_layout, patch_var_column_in_place, RowLayout,
};
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
    /// from the catalog file). Indexes are NOT rebuilt — they live in memory
    /// until `create_index` is called again. Prefer `open_with_indexes` when
    /// the catalog knows which columns are indexed.
    pub fn open(schema: Schema, data_dir: &Path) -> io::Result<Self> {
        Self::open_with_indexes(schema, data_dir, &[])
    }

    /// Mission 3: reopen an existing table from disk, also rehydrating any
    /// persisted b-tree indexes.
    ///
    /// For each name in `indexed_col_names`:
    ///   - If the `{table}_{col}.idx` file exists, load it via
    ///     `BTree::load` — O(file size) memcpy+decode, no heap scan.
    ///   - If the file is missing (e.g. first open after upgrading from
    ///     pre-Mission-3 catalogs), fall back to the create-time rebuild
    ///     path: scan the heap and insert every non-empty value. After the
    ///     rebuild, `save` the freshly built tree so subsequent opens hit
    ///     the fast path.
    pub fn open_with_indexes(
        schema: Schema,
        data_dir: &Path,
        indexed_col_names: &[String],
    ) -> io::Result<Self> {
        let heap_path = data_dir.join(format!("{}.heap", schema.table_name));
        let heap = HeapFile::open(&heap_path)?;
        let row_layout = RowLayout::new(&schema);
        let mut table = Table {
            schema,
            heap,
            encode_scratch: Vec::new(),
            indexed_cols: Vec::new(),
            row_layout,
        };

        for col_name in indexed_col_names {
            let col_idx = match table.schema.column_index(col_name) {
                Some(i) => i,
                // Schema drift: the catalog lists an index on a column that
                // no longer exists. Silently drop the index rather than
                // failing the whole open — matches the `drop column`
                // rewrite path, which already blows away indexes.
                None => continue,
            };
            let is_int = table.schema.columns[col_idx].type_id == TypeId::Int;
            let idx_path = data_dir.join(format!(
                "{}_{}.idx",
                table.schema.table_name, col_name
            ));

            let btree = if idx_path.exists() {
                BTree::load(&idx_path)?
            } else {
                // Missing file: rebuild from the heap and save so we
                // take the fast path next time.
                let mut bt = BTree::create(&idx_path)?;
                for (rid, row) in table.heap.scan() {
                    let row = crate::row::decode_row(&table.schema, &row);
                    if !row[col_idx].is_empty() {
                        bt.insert(row[col_idx].clone(), rid);
                    }
                }
                bt.save()?;
                bt
            };

            table.indexed_cols.push(IndexedCol {
                col_idx,
                col_name: col_name.clone(),
                is_int,
                btree,
            });
        }

        Ok(table)
    }

    /// Mission 3: catalog uses this to snapshot the list of columns that
    /// currently have an index, so it can be persisted in `catalog.bin`.
    pub(crate) fn indexed_column_names(&self) -> Vec<String> {
        self.indexed_cols.iter().map(|c| c.col_name.clone()).collect()
    }

    /// Recalculate the cached row layout from the current schema. Must be
    /// called after any schema mutation (add/drop column).
    pub fn refresh_layout(&mut self) {
        self.row_layout = RowLayout::new(&self.schema);
    }

    /// Rewrite every live heap row to match a new schema shape.
    ///
    /// This is the backfill path for `ALTER TABLE ADD COLUMN`. Before
    /// this existed, the catalog happily swapped the schema in memory
    /// and left old rows on disk with the OLD variable-column offset
    /// table layout. Any subsequent `decode_row` then panicked with
    /// `range end index X out of range` because the decoder reads
    /// `n_var + 1` offsets using the NEW schema.
    ///
    /// The caller passes in the pre-mutation schema so rows can be
    /// decoded correctly; `self.schema` must already hold the NEW
    /// schema when this is invoked. `fill_values` must have
    /// `new_schema.columns.len()` entries and supplies the values for
    /// columns that did not exist in the old schema (use
    /// `Value::Empty` for optional adds).
    ///
    /// Rewrites every row via `HeapFile::update`, which may move the
    /// row to a new page when the new encoding is larger. Any secondary
    /// indexes are rebuilt from scratch at the end because their
    /// `RowId` pointers can become stale during the rewrite.
    ///
    /// Not on any hot path — ALTER is a rare administrative op, so this
    /// intentionally prefers simplicity (collect snapshot → rewrite →
    /// rebuild indexes) over any of the fast-path tricks used by
    /// insert/update/delete.
    pub(crate) fn rewrite_rows_for_schema_change(
        &mut self,
        old_schema: &Schema,
        fill_values: &[Value],
        data_dir: &Path,
    ) -> io::Result<()> {
        debug_assert_eq!(fill_values.len(), self.schema.columns.len());

        // Snapshot every live (rid, old_bytes) pair up front. We can't
        // mutate `self.heap` while iterating it, and the rewrite grows
        // every row (+2 bytes of offset table at minimum), so in-place
        // updates are not guaranteed.
        let snapshot: Vec<(RowId, Vec<u8>)> = self.heap.scan().collect();

        // Map from old column index → new column index, or `None` if
        // the old column was dropped by the schema change. The caller
        // is expected to keep surviving columns in their original
        // positions. We look up by name so ADD and DROP can share the
        // same path: ADD has every old column present in the new
        // schema; DROP has exactly one missing.
        let old_to_new: Vec<Option<usize>> = old_schema
            .columns
            .iter()
            .map(|c| self.schema.column_index(&c.name))
            .collect();

        for (rid, old_bytes) in snapshot {
            let old_row = decode_row(old_schema, &old_bytes);
            // Start from the caller-supplied defaults for the new
            // shape, then overwrite with whatever the old row had.
            // Dropped columns are simply skipped (their value has
            // nowhere to go in the new row).
            let mut new_row: Vec<Value> = fill_values.to_vec();
            for (old_idx, val) in old_row.into_iter().enumerate() {
                if let Some(new_idx) = old_to_new[old_idx] {
                    new_row[new_idx] = val;
                }
            }

            encode_row_into_with_layout(
                &self.schema,
                &self.row_layout,
                &new_row,
                &mut self.encode_scratch,
            );
            // We don't care about the new RowId here: any secondary
            // index is rebuilt from scratch below.
            self.heap.update(rid, &self.encode_scratch)?;
        }

        // Rebuild every secondary index from the rewritten heap. The
        // in-memory btree is the source of truth for reads, and its
        // RowId pointers may now be stale after the heap rewrite.
        if !self.indexed_cols.is_empty() {
            // Preserve per-index metadata (col_idx, col_name, is_int)
            // via fresh BTree instances. The old btrees are dropped
            // when `indexed_cols` is reassigned.
            let existing: Vec<(usize, String, bool)> = self
                .indexed_cols
                .iter()
                .map(|c| (c.col_idx, c.col_name.clone(), c.is_int))
                .collect();

            // Drain the old entries first so the borrow of
            // `self.indexed_cols` is clear before we start scanning.
            self.indexed_cols.clear();

            for (col_idx, col_name, is_int) in existing {
                // Mission 3: write the freshly rebuilt index back to its
                // canonical `{table}_{col}.idx` file so a subsequent
                // restart loads the up-to-date tree instead of the stale
                // pre-rewrite version (whose RowIds may now point at
                // moved rows).
                let idx_path = data_dir.join(format!(
                    "{}_{}.idx",
                    self.schema.table_name, col_name
                ));
                let mut btree = crate::btree::BTree::create(&idx_path)?;
                for (rid, row) in self.heap.scan() {
                    let row = decode_row(&self.schema, &row);
                    let v = &row[col_idx];
                    if v.is_empty() {
                        continue;
                    }
                    if is_int {
                        if let Value::Int(i) = v {
                            btree.insert_int(*i, rid);
                            continue;
                        }
                    }
                    btree.insert(v.clone(), rid);
                }
                btree.save()?;
                self.indexed_cols.push(IndexedCol {
                    col_idx,
                    col_name,
                    is_int,
                    btree,
                });
            }
        }

        Ok(())
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
        encode_row_into_with_layout(
            &self.schema,
            &self.row_layout,
            values,
            &mut self.encode_scratch,
        );
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
        //
        // Blocker B3: each `insert` / `insert_int` flips the btree's
        // dirty flag in memory; the actual `save` (serialize + fsync
        // + rename) is deferred to the next `Catalog::checkpoint` /
        // `Catalog::drop`. Mission 3 used to do one fsync per row
        // here, which cost `insert_batch_1k` ~1000 fsyncs per
        // iteration and wiped out the D10/D11 wins.
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

    /// Blocker B3: flush every dirty btree index to disk. Wired into
    /// [`crate::catalog::Catalog::checkpoint`] and its `Drop` impl so
    /// we get one fsync + rename per dirty index per checkpoint, not
    /// one per inserted row. Clean trees (no mutations since last
    /// save) are free — `BTree::save_if_dirty` early-returns.
    pub(crate) fn save_dirty_indexes(&mut self) -> io::Result<()> {
        for entry in self.indexed_cols.iter_mut() {
            entry.btree.save_if_dirty()?;
        }
        Ok(())
    }

    /// Blocker B3: rebuild every secondary index from the heap.
    ///
    /// Used by the crash-recovery path in `Catalog::open`: after WAL
    /// replay lands rows back in the heap, the on-disk `.idx` files
    /// may lag (or lead) the heap because the prior session deferred
    /// btree saves until checkpoint. Replaying is cheap — we walk the
    /// heap once per index — and produces a tree that exactly matches
    /// the current heap state, which is the invariant subsequent
    /// inserts assume.
    ///
    /// After this call, every indexed tree is marked dirty so the
    /// next `Catalog::checkpoint` persists the recovered state.
    pub(crate) fn rebuild_indexes_from_heap(&mut self) -> io::Result<()> {
        if self.indexed_cols.is_empty() {
            return Ok(());
        }

        let schema = &self.schema;
        for entry in self.indexed_cols.iter_mut() {
            let mut fresh = BTree::create(entry.btree.file_path())?;
            for (rid, row) in self.heap.scan() {
                let row = crate::row::decode_row(schema, &row);
                let v = &row[entry.col_idx];
                if v.is_empty() {
                    continue;
                }
                if entry.is_int {
                    if let Value::Int(i) = v {
                        fresh.insert_int(*i, rid);
                        continue;
                    }
                }
                fresh.insert(v.clone(), rid);
            }
            // Force-mark dirty so the next checkpoint flushes the
            // freshly rebuilt tree, even if no further mutations
            // happen before shutdown.
            fresh.mark_dirty();
            entry.btree = fresh;
        }
        Ok(())
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

        self.heap.delete(rid)?;
        // Blocker B3: btree mutations above marked the indexes dirty.
        // The actual persist happens at the next `Catalog::checkpoint`
        // (or `Drop`), batching many deletes into one fsync per index.
        Ok(())
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

        // Blocker B3: indexes are now dirty in memory; `delete_many_int`
        // already flipped the dirty flag on each mutated btree above.
        // Checkpoint batches the persist.

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
        self.scan_delete_matching_with_hook(pred, |_, _| {})
    }

    /// Variant of [`Self::scan_delete_matching`] that lets the caller
    /// observe every matched row just before it's marked deleted. Used
    /// by [`crate::catalog::Catalog::scan_delete_matching_logged`] to
    /// emit one WAL `Delete` record per victim in the same single-pass
    /// scan — no second walk over the heap, no per-row `ensure_hot`
    /// round-trip.
    ///
    /// The user hook runs inside the heap's pinned hot-page borrow, so
    /// it must not call back into the catalog / table / heap. The WAL
    /// append path only writes into an in-memory buffer and is safe.
    pub fn scan_delete_matching_with_hook<P, H>(
        &mut self,
        pred: P,
        mut user_hook: H,
    ) -> io::Result<u64>
    where
        P: FnMut(&[u8]) -> bool,
        H: FnMut(RowId, &[u8]),
    {
        if self.indexed_cols.is_empty() {
            return self.heap.scan_delete_matching(pred, |rid, bytes| {
                user_hook(rid, bytes);
            });
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

            let count = heap.scan_delete_matching(pred, |rid, data| {
                for (slot_i, entry) in indexed_cols.iter().enumerate() {
                    if let Value::Int(i) = decode_column(schema, layout, data, entry.col_idx) {
                        keys_per_index[slot_i].push(i);
                    }
                }
                user_hook(rid, data);
            })?;

            // Mission C Phase 17: btrees live inline in indexed_cols,
            // so this direct iter_mut replaces the old HashMap probe.
            for (slot_i, entry) in indexed_cols.iter_mut().enumerate() {
                let keys = &mut keys_per_index[slot_i];
                keys.sort_unstable();
                entry.btree.delete_many_int(keys);
            }
            // Blocker B3: dirty flags are already set by the
            // per-btree `delete_many_int` call above; checkpoint
            // handles the persist.
            return Ok(count);
        }

        // Mixed / non-int secondary indexes: still do the single heap
        // pass, but fall back to per-key btree deletes at the end.
        let mut values_per_index: Vec<Vec<Value>> =
            (0..n_indexed).map(|_| Vec::with_capacity(256)).collect();

        let count = heap.scan_delete_matching(pred, |rid, data| {
            for (slot_i, entry) in indexed_cols.iter().enumerate() {
                let v = decode_column(schema, layout, data, entry.col_idx);
                if !v.is_empty() {
                    values_per_index[slot_i].push(v);
                }
            }
            user_hook(rid, data);
        })?;

        for (slot_i, entry) in indexed_cols.iter_mut().enumerate() {
            for v in &values_per_index[slot_i] {
                entry.btree.delete(v);
            }
        }
        // Blocker B3: btree dirty flags are set by `delete`; checkpoint
        // flushes later.
        Ok(count)
    }

    /// Single-pass fused scan + in-place patch. Evaluates `pred` on raw
    /// row bytes and applies `try_mutate` to each match on the same hot
    /// page — no second pass. Returns `(patched_count, fallback_rids)`.
    ///
    /// The `hook` closure fires after each successful patch with the
    /// post-mutation bytes, used for WAL logging.
    ///
    /// Perf sprint: this is the update analogue of
    /// `scan_delete_matching_with_hook`. Eliminates the two-pass
    /// collect-then-patch pattern that doubled `ensure_hot` calls for
    /// `update_by_filter`.
    pub fn scan_patch_matching_with_hook<P, M, H>(
        &mut self,
        pred: P,
        try_mutate: M,
        hook: H,
    ) -> io::Result<(u64, Vec<RowId>)>
    where
        P: FnMut(&[u8]) -> bool,
        M: FnMut(&mut [u8]) -> Option<u16>,
        H: FnMut(RowId, &[u8]),
    {
        // No index maintenance needed — callers guarantee the patched
        // columns are not indexed (same constraint as the per-rid
        // `with_row_bytes_mut` / `patch_var_col_in_place` fast paths).
        self.heap.scan_patch_matching(pred, try_mutate, hook)
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
    ///
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

        encode_row_into_with_layout(
            &self.schema,
            &self.row_layout,
            values,
            &mut self.encode_scratch,
        );
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
        // Blocker B3: any mutated btree is now dirty; checkpoint will
        // persist it. No per-row fsync on this hot path.
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

        // Mission 3: persist the freshly-built index so it survives a
        // restart. `BTree::create` stashed the path inside the tree, so
        // `save()` writes to the right place. Subsequent inserts / updates
        // / deletes will re-save after each mutation (see `save_if_touched`).
        btree.save()?;

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
