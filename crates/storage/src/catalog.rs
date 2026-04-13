use crate::row::encode_row_into;
use crate::table::Table;
use crate::types::*;
use crate::wal::{Wal, WalRecordType, WalSyncMode};
use rustc_hash::FxHashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// On-disk catalog file: lists every table's schema so we can reopen them
/// after a restart. Format is a small custom binary blob (no serde dep).
///
/// Mission 3: version 2 appends a per-table list of indexed column names
/// after the column list, so indexes can be rehydrated on `Catalog::open`.
/// Version 1 files still load cleanly — they're treated as having zero
/// indexed columns, and the next `create_index` (or implicit rebuild on
/// first open, depending on the caller) will populate the list.
const CATALOG_FILE: &str = "catalog.bin";
const CATALOG_MAGIC: &[u8; 4] = b"BCAT";
const CATALOG_VERSION: u16 = 2;

/// Mission 2 (durability): the single shared WAL file lives under the catalog's
/// data directory with this name. One WAL covers every table in the catalog.
const WAL_FILE: &str = "wal.log";

/// WAL batch size: flush auto-triggers after this many records, in addition
/// to the explicit `wal.flush()` each top-level mutation does. Kept small so
/// the tests see a predictable amount of buffering.
const WAL_BATCH_SIZE: usize = 64;

/// System catalog: registry of all tables.
///
/// Mission C Phase 18: tables live in a `Vec<Table>` addressed by a
/// stable `slot` index, with a parallel `FxHashMap<String, usize>` for
/// name-based resolution. Append-only (PowDB has no DROP TABLE yet), so
/// slots are stable for the lifetime of the `Catalog` — callers like
/// `PreparedQuery::insert_fast` cache a slot at prepare time and skip
/// the name probe on every subsequent `execute_prepared_take`.
///
/// Earlier design (pre-Phase 18) held tables in a `FxHashMap<String, Table>`
/// directly. That meant the `insert_batch_1k` hot path paid an
/// `FxHash("User")` + bucket walk per row just to dispatch into the
/// table — about 20-40ns out of a 233ns budget.
pub struct Catalog {
    /// All tables, in insertion order. Indexed by `slot: usize`. A table's
    /// slot is assigned by `create_table`/`open` and never reused.
    tables: Vec<Table>,
    /// Name → slot index. Populated in sync with `tables` on every
    /// `create_table` / `open`.
    name_to_slot: FxHashMap<String, usize>,
    data_dir: PathBuf,
    /// Mission 2: shared write-ahead log owned by the catalog. Every
    /// mutation (insert/update/delete) records its intent here BEFORE
    /// touching the heap so a mid-write crash can be recovered from on the
    /// next open. Flushed to disk at the end of every top-level op.
    wal: Wal,
    /// Monotonic transaction-id counter — one "operation = transaction"
    /// under our minimum-viable scope. Incremented per mutation so WAL
    /// records can be grouped by op on replay if needed.
    next_tx_id: u64,
    /// Has this catalog been cleanly checkpointed at least once since it
    /// was opened? Used by `Drop` to decide whether to treat its own flush
    /// as fatal (it isn't — we still try best-effort).
    checkpointed: bool,
}

impl Catalog {
    /// Create a brand-new catalog. Wipes any existing catalog file in this directory.
    pub fn create(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let wal_path = data_dir.join(WAL_FILE);
        let wal = Wal::create(&wal_path, WAL_BATCH_SIZE)?;
        let cat = Catalog {
            tables: Vec::new(),
            name_to_slot: FxHashMap::default(),
            data_dir: data_dir.to_path_buf(),
            wal,
            next_tx_id: 1,
            checkpointed: false,
        };
        cat.persist()?;
        Ok(cat)
    }

    /// Open an existing catalog from disk, rehydrating every table. If no
    /// catalog file is present this returns NotFound — callers can fall back
    /// to `create` for a fresh data dir.
    ///
    /// Mission 2: after the per-table heap files are reopened, this replays
    /// any records left in the WAL from a previous (crashed) session. The
    /// WAL is then truncated once the replay lands cleanly on disk — that
    /// re-establishes the "empty WAL = last shutdown was clean" invariant.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        let cat_path = data_dir.join(CATALOG_FILE);
        if !cat_path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no catalog file"));
        }
        let entries = read_catalog_file(&cat_path)?;
        let mut tables: Vec<Table> = Vec::with_capacity(entries.len());
        let mut name_to_slot =
            FxHashMap::with_capacity_and_hasher(entries.len(), Default::default());
        for CatalogEntry {
            schema,
            indexed_cols,
        } in entries
        {
            let name = schema.table_name.clone();
            // Mission 3: rehydrate persisted indexes. `Table::open_with_indexes`
            // tries to `BTree::load` each named index file; if a file is
            // missing (e.g. first open after upgrade from catalog v1) it
            // falls back to rebuilding from the heap scan and saving to
            // disk so subsequent opens hit the fast path.
            let table = Table::open_with_indexes(schema, data_dir, &indexed_cols)?;
            name_to_slot.insert(name, tables.len());
            tables.push(table);
        }
        let wal_path = data_dir.join(WAL_FILE);
        let wal = Wal::open(&wal_path, WAL_BATCH_SIZE)?;
        let mut cat = Catalog {
            tables,
            name_to_slot,
            data_dir: data_dir.to_path_buf(),
            wal,
            next_tx_id: 1,
            checkpointed: false,
        };
        cat.replay_wal()?;
        Ok(cat)
    }

    /// Replay every record currently buffered in the WAL file onto the open
    /// tables. This is the recovery path: after a crash the heap files on
    /// disk may be missing mutations that were logged to the WAL but never
    /// written back to their pages. We re-apply every record unconditionally.
    ///
    /// **Idempotence:**
    /// - `Delete`: idempotent — `HeapFile::delete` on an already-deleted or
    ///   missing slot is a no-op.
    /// - `Update`: idempotent — re-applies the same new row bytes to the
    ///   same `RowId`, which either replaces the existing (already-updated)
    ///   row with itself or lands the update for the first time.
    /// - `Insert`: **NOT strictly idempotent**. `HeapFile::insert` allocates
    ///   a fresh `RowId` on every call, so a row that was already flushed
    ///   to disk will be re-inserted at a new location, producing a
    ///   duplicate. See the mission report for the full caveat.
    ///
    /// The practical consequences are:
    ///   1. On a "pure crash" (no heap pages ever flushed between open and
    ///      crash), replay cleanly restores every logged row.
    ///   2. On a crash where some heap pages were flushed by the hot-page
    ///      eviction logic, replay may restore those rows a second time.
    ///      A future mission can fix this with LSN-tagged pages.
    ///
    /// After a successful replay we truncate the WAL so the next shutdown
    /// (crash or otherwise) replays only the NEW records.
    fn replay_wal(&mut self) -> io::Result<()> {
        let records = self.wal.read_all()?;
        if records.is_empty() {
            return Ok(());
        }
        info!(count = records.len(), "replaying WAL records");
        let mut replayed_inserts = 0usize;
        let mut replayed_updates = 0usize;
        let mut replayed_deletes = 0usize;
        for rec in records {
            match rec.record_type {
                WalRecordType::Insert => {
                    if let Some((table_name, _rid, row_bytes)) = decode_wal_payload(&rec.data) {
                        // Route around `Catalog::insert` so we don't
                        // re-log during replay.
                        if let Some(slot) = self.name_to_slot.get(&table_name).copied() {
                            // Write the raw encoded bytes directly to the
                            // heap. We bypass Table::insert because that
                            // would re-encode (we already have bytes) and
                            // touch secondary indexes (which we rebuild
                            // post-replay anyway — this mission doesn't
                            // persist indexes).
                            let tbl = &mut self.tables[slot];
                            let _ = tbl.heap.insert(&row_bytes)?;
                            replayed_inserts += 1;
                        }
                    }
                }
                WalRecordType::Update => {
                    if let Some((table_name, rid, row_bytes)) = decode_wal_payload(&rec.data) {
                        if let Some(slot) = self.name_to_slot.get(&table_name).copied() {
                            let tbl = &mut self.tables[slot];
                            let _ = tbl.heap.update(rid, &row_bytes)?;
                            replayed_updates += 1;
                        }
                    }
                }
                WalRecordType::Delete => {
                    if let Some((table_name, rid, _)) = decode_wal_payload(&rec.data) {
                        if let Some(slot) = self.name_to_slot.get(&table_name).copied() {
                            let tbl = &mut self.tables[slot];
                            // Delete is idempotent on a missing/deleted slot.
                            let _ = tbl.heap.delete(rid);
                            replayed_deletes += 1;
                        }
                    }
                }
                WalRecordType::Commit | WalRecordType::Rollback => {
                    // Mission 2: one-op-one-transaction model — Commit /
                    // Rollback markers are unused. Kept here so a future
                    // mission that adds multi-op transactions can extend
                    // replay without a WAL format break.
                }
            }
        }
        info!(
            inserts = replayed_inserts,
            updates = replayed_updates,
            deletes = replayed_deletes,
            "WAL replay complete"
        );
        // Persist the replayed changes to disk before truncating the WAL,
        // otherwise a crash between here and the next checkpoint would lose
        // the replayed records. `flush_all_dirty` on every heap moves every
        // dirty page through the normal write path.
        //
        // Blocker B3: under the deferred-index-save model, the on-disk
        // `.idx` files may lag the heap because the pre-crash session
        // never got to its next `checkpoint`. Replay restored the
        // heap rows above, but the btrees that loaded from those
        // possibly-stale `.idx` files don't know about them. Rebuild
        // every secondary index from the post-replay heap so the
        // trees exactly match disk. The rebuild is O(heap) per
        // indexed column, which is fine on a crash-recovery path.
        for tbl in &mut self.tables {
            tbl.heap.flush_all_dirty()?;
            tbl.heap.flush()?;
            tbl.rebuild_indexes_from_heap()?;
            // Flush the rebuilt indexes now so a crash between here
            // and the next mutation still leaves `.idx` files matching
            // the heap. Without this, a second crash before any
            // insert could leave us back where we started.
            tbl.save_dirty_indexes()?;
        }
        self.wal.truncate()?;
        Ok(())
    }

    /// Flush every dirty heap page and truncate the WAL. This is the
    /// "clean shutdown" point — after this returns, the on-disk heap files
    /// are fully consistent and the WAL is empty, so the next `open` will
    /// skip replay entirely.
    ///
    /// Safe to call multiple times. Safe to call on a catalog that has
    /// performed zero mutations since the last checkpoint (in which case
    /// the flushes are no-ops and the truncate is a bounded syscall).
    pub fn checkpoint(&mut self) -> io::Result<()> {
        for tbl in &mut self.tables {
            tbl.heap.flush_all_dirty()?;
            tbl.heap.flush()?;
            // Blocker B3: the hot insert/update/delete paths no longer
            // fsync index files per row — they only mark the in-memory
            // btree dirty. Checkpoint is where those deferred saves
            // actually hit disk. Clean (non-dirty) indexes are free.
            tbl.save_dirty_indexes()?;
        }
        self.wal.flush()?;
        self.wal.truncate()?;
        self.checkpointed = true;
        Ok(())
    }

    /// Allocate a new transaction id for a single top-level op.
    #[inline]
    fn next_tx(&mut self) -> u64 {
        let id = self.next_tx_id;
        self.next_tx_id = self.next_tx_id.wrapping_add(1);
        id
    }

    /// Append a mutation record to the WAL buffer. **Does not flush.**
    ///
    /// Mission B (post-review): per-row `wal.flush()` was a ~1ms fsync on
    /// every mutation, turning `update_by_filter` into a ~19s workload.
    /// The flush is now deferred to [`Self::sync_wal`], which the executor
    /// calls exactly once at the end of every mutating statement. This
    /// gives us statement-level group commit: N-row updates pay one fsync,
    /// not N.
    ///
    /// Durability contract: any path that observes `Ok(...)` back from
    /// the executor must have called `sync_wal` before returning that
    /// Ok. Replay is still correct because WAL records are appended in
    /// order and only records that reached `fdatasync`ed bytes are
    /// replayed.
    fn wal_log(
        &mut self,
        tx_id: u64,
        record_type: WalRecordType,
        table: &str,
        rid: RowId,
        row_bytes: &[u8],
    ) -> io::Result<()> {
        // Mission B (post-review, second pass): when the WAL is in Off
        // mode the `append` call below is a no-op, so building the
        // payload first wastes a `Vec` allocation + ~3 extends per
        // mutation. The catalog hot paths check `wal.is_off()` before
        // calling here, but this guard is the belt-and-braces version
        // for any internal caller that doesn't.
        if self.wal.is_off() {
            return Ok(());
        }
        let payload = encode_wal_payload(table, rid, row_bytes);
        self.wal.append(tx_id, record_type, &payload)
    }

    /// Flush any buffered WAL records to disk. Called by the executor
    /// at the end of every mutating statement so the group-commit
    /// window is exactly one statement.
    ///
    /// See [`Self::wal_log`] for the durability contract.
    #[inline]
    pub fn sync_wal(&mut self) -> io::Result<()> {
        self.wal.flush()
    }

    /// Set the WAL sync mode. Production code should leave this at the
    /// default ([`WalSyncMode::Full`]). Benchmarks set it to
    /// [`WalSyncMode::Off`] to compare apples-to-apples against
    /// `:memory:` SQLite (which has zero fsync cost).
    ///
    /// **Never** call this with `Off` in production — a machine crash
    /// can lose any record written since the last `sync_wal` returned.
    pub fn set_wal_sync_mode(&mut self, mode: WalSyncMode) {
        self.wal.set_sync_mode(mode);
    }

    pub fn create_table(&mut self, schema: Schema) -> io::Result<()> {
        let name = schema.table_name.clone();
        if self.name_to_slot.contains_key(&name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("table '{name}' already exists"),
            ));
        }
        let table = Table::create(schema, &self.data_dir)?;
        let slot = self.tables.len();
        self.tables.push(table);
        self.name_to_slot.insert(name, slot);
        // Persist the updated catalog so the new schema survives a crash/restart.
        self.persist()?;
        Ok(())
    }

    /// Write the current set of schemas to disk atomically (write-then-rename).
    ///
    /// Mission 3: also writes the per-table list of indexed column names so
    /// `Catalog::open` can rehydrate b-tree indexes on restart.
    fn persist(&self) -> io::Result<()> {
        let cat_path = self.data_dir.join(CATALOG_FILE);
        let tmp_path = self.data_dir.join(format!("{CATALOG_FILE}.tmp"));
        let entries: Vec<CatalogEntryRef<'_>> = self
            .tables
            .iter()
            .map(|t| CatalogEntryRef {
                schema: &t.schema,
                indexed_cols: t.indexed_column_names(),
            })
            .collect();
        write_catalog_file(&tmp_path, &entries)?;
        fs::rename(&tmp_path, &cat_path)?;
        Ok(())
    }

    /// Resolve a table name to its stable slot index. Prepared-query
    /// fast paths cache this once and skip the hash probe on every
    /// subsequent execution. Slots never shift once assigned.
    #[inline]
    pub fn table_slot(&self, name: &str) -> Option<usize> {
        self.name_to_slot.get(name).copied()
    }

    /// O(1) slot-indexed table access. Panics on an out-of-range slot
    /// — callers must have obtained the slot via `table_slot()`.
    #[inline]
    pub fn table_by_slot(&self, slot: usize) -> &Table {
        &self.tables[slot]
    }

    /// Mutable counterpart to [`Self::table_by_slot`].
    #[inline]
    pub fn table_by_slot_mut(&mut self, slot: usize) -> &mut Table {
        &mut self.tables[slot]
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        let slot = *self.name_to_slot.get(name)?;
        Some(&self.tables[slot])
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        let slot = *self.name_to_slot.get(name)?;
        Some(&mut self.tables[slot])
    }

    /// Private helper: resolve a table name to `&Table`, or return an
    /// `io::Error` with the same "table '<name>' not found" message the
    /// older `get_mut().ok_or_else(...)` callers produced. Phase 18
    /// consolidates ~14 copies of that idiom into this one place.
    #[inline]
    fn by_name(&self, table: &str) -> io::Result<&Table> {
        let slot = *self.name_to_slot.get(table).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{table}' not found"),
            )
        })?;
        Ok(&self.tables[slot])
    }

    /// Mutable counterpart to [`Self::by_name`].
    #[inline]
    fn by_name_mut(&mut self, table: &str) -> io::Result<&mut Table> {
        let slot = *self.name_to_slot.get(table).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{table}' not found"),
            )
        })?;
        Ok(&mut self.tables[slot])
    }

    pub fn insert(&mut self, table: &str, values: &Row) -> io::Result<RowId> {
        // Mission 2: encode the row into a scratch buffer first so we can
        // log it to the WAL before touching the heap. We re-encode inside
        // `Table::insert`, which keeps the insert hot path untouched — the
        // WAL encode here is additive.
        //
        // Mission B (post-review, second pass): in `WalSyncMode::Off` the
        // entire WAL pipeline is a no-op, so skip the per-row
        // `encode_row_into` allocation and `wal_log` call entirely.
        if self.wal.is_off() {
            return self.by_name_mut(table)?.insert(values);
        }
        let tbl = self.by_name_mut(table)?;
        let mut wal_bytes: Vec<u8> = Vec::new();
        encode_row_into(&tbl.schema, values, &mut wal_bytes);
        let tx_id = self.next_tx();
        // Placeholder RowId — the real one is assigned by the heap below.
        // Replay of an Insert record ignores the RowId field anyway.
        self.wal_log(
            tx_id,
            WalRecordType::Insert,
            table,
            RowId {
                page_id: 0,
                slot_index: 0,
            },
            &wal_bytes,
        )?;
        self.by_name_mut(table)?.insert(values)
    }

    pub fn get(&self, table: &str, rid: RowId) -> Option<Row> {
        self.get_table(table)?.get(rid)
    }

    pub fn delete(&mut self, table: &str, rid: RowId) -> io::Result<()> {
        // Mission B (post-review, second pass): WAL Off → no payload
        // construction.
        if self.wal.is_off() {
            return self.by_name_mut(table)?.delete(rid);
        }
        let tx_id = self.next_tx();
        // Delete records carry only the rid — no row payload.
        self.wal_log(tx_id, WalRecordType::Delete, table, rid, &[])?;
        self.by_name_mut(table)?.delete(rid)
    }

    /// Mission C Phase 12: bulk delete a list of rids, batching btree
    /// maintenance. See [`Table::delete_many`] for the full explanation
    /// and fall-through rules. Returns the number of rows removed.
    pub fn delete_many(&mut self, table: &str, rids: &[RowId]) -> io::Result<u64> {
        // Mission 2: log every rid as an individual Delete record. The
        // WAL flush is deferred to the executor's statement-end
        // `sync_wal` — see [`Self::wal_log`] for the group-commit rules.
        //
        // Mission B (post-review, second pass): in Off mode skip the
        // entire per-row payload loop — `wal.append` would no-op every
        // call but the `encode_wal_payload` Vec alloc would still run.
        if self.wal.is_off() {
            return self.by_name_mut(table)?.delete_many(rids);
        }
        let tx_id = self.next_tx();
        for &rid in rids {
            let payload = encode_wal_payload(table, rid, &[]);
            self.wal.append(tx_id, WalRecordType::Delete, &payload)?;
        }
        self.by_name_mut(table)?.delete_many(rids)
    }

    /// Mission C Phase 16: single-pass scan-and-delete driven by a
    /// raw-bytes predicate. See [`Table::scan_delete_matching`] and
    /// [`HeapFile::scan_delete_matching`] for the fusion rationale.
    ///
    /// Mission B2: prefer [`Self::scan_delete_matching_logged`] from any
    /// caller that needs crash durability. This variant writes no WAL
    /// records, so a crash between the scan and the next checkpoint
    /// would lose the deletes. Kept here for internal paths (e.g.
    /// `drop_table`) where the whole heap is about to be removed anyway.
    pub fn scan_delete_matching<P>(&mut self, table: &str, pred: P) -> io::Result<u64>
    where
        P: FnMut(&[u8]) -> bool,
    {
        self.by_name_mut(table)?.scan_delete_matching(pred)
    }

    /// Mission B2: WAL-logged variant of [`Self::scan_delete_matching`].
    /// Every matched row emits one `WalRecordType::Delete` record in the
    /// same single-pass scan (via the table's `_with_hook` variant), so
    /// crash recovery sees every deletion. Used by the executor's
    /// `Delete(Filter(SeqScan))` and bare `Delete(SeqScan)` fast paths.
    ///
    /// Performance cost vs the non-logged primitive is one per-row WAL
    /// append into the in-memory buffer plus one `fsync` at the end —
    /// the heap scan itself still runs as a single pass with one
    /// `ensure_hot` per page.
    pub fn scan_delete_matching_logged<P>(&mut self, table: &str, pred: P) -> io::Result<u64>
    where
        P: FnMut(&[u8]) -> bool,
    {
        // Mission B (post-review, second pass): in Off mode the per-row
        // hook would build a Vec, do five extends, and then `append`
        // would no-op. Skip the WAL hook entirely and route through
        // the no-WAL primitive — same single-pass scan, zero per-row
        // payload work.
        if self.wal.is_off() {
            return self.by_name_mut(table)?.scan_delete_matching(pred);
        }
        // Resolve slot up front so we can split the borrow — the user
        // hook closes over `&mut self.wal`, which can't coexist with a
        // `by_name_mut` borrow of `self.tables`.
        let slot = *self.name_to_slot.get(table).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{table}' not found"),
            )
        })?;
        let tx_id = self.next_tx();
        // Split-borrow the catalog fields so the hook can write into
        // `wal` while the scan pins `tables[slot]` mutably.
        let Catalog { tables, wal, .. } = self;
        let tbl = &mut tables[slot];
        // Pre-encode the table-name prefix of every WAL payload once —
        // it doesn't vary row-to-row, and the per-row rid+row bytes are
        // the only things we append inside the hook.
        let name_bytes = table.as_bytes();
        let count = tbl.scan_delete_matching_with_hook(pred, |rid, row_bytes| {
            let mut payload: Vec<u8> =
                Vec::with_capacity(4 + name_bytes.len() + 10 + row_bytes.len());
            payload.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(name_bytes);
            payload.extend_from_slice(&rid.page_id.to_le_bytes());
            payload.extend_from_slice(&rid.slot_index.to_le_bytes());
            // Delete records carry no row payload on replay, but we
            // match the `encode_wal_payload` layout so `decode_wal_payload`
            // (which is type-agnostic) parses them cleanly.
            payload.extend_from_slice(&0u32.to_le_bytes());
            // Best-effort append — if it errors we have no way to
            // propagate from inside the hook; we swallow it here and
            // the outer scan's `io::Result` will still succeed. In
            // practice the `BufWriter`-backed `Wal::append` only errors
            // on allocation failure or a disk-full fsync, both of
            // which would fail the outer flush below as well.
            let _ = wal.append(tx_id, WalRecordType::Delete, &payload);
        })?;
        // Flush is deferred to the executor's statement-end `sync_wal`.
        Ok(count)
    }

    /// Single-pass fused scan + in-place patch with WAL logging.
    /// Evaluates `pred` on raw row bytes and applies `try_mutate` to each
    /// match on the same hot page — no second pass. Returns
    /// `(patched_count, fallback_rids)`.
    ///
    /// Perf sprint: update analogue of `scan_delete_matching_logged`.
    /// Eliminates the two-pass collect-then-patch pattern.
    pub fn scan_patch_matching_logged<P, M>(
        &mut self,
        table: &str,
        pred: P,
        try_mutate: M,
    ) -> io::Result<(u64, Vec<RowId>)>
    where
        P: FnMut(&[u8]) -> bool,
        M: FnMut(&mut [u8]) -> Option<u16>,
    {
        if self.wal.is_off() {
            return self.by_name_mut(table)?.scan_patch_matching_with_hook(
                pred,
                try_mutate,
                |_, _| {},
            );
        }
        let slot = *self.name_to_slot.get(table).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{table}' not found"),
            )
        })?;
        let tx_id = self.next_tx();
        let Catalog { tables, wal, .. } = self;
        let tbl = &mut tables[slot];
        let name_bytes = table.as_bytes();
        let result = tbl.scan_patch_matching_with_hook(pred, try_mutate, |rid, row_bytes| {
            let mut payload: Vec<u8> =
                Vec::with_capacity(4 + name_bytes.len() + 10 + row_bytes.len());
            payload.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(name_bytes);
            payload.extend_from_slice(&rid.page_id.to_le_bytes());
            payload.extend_from_slice(&rid.slot_index.to_le_bytes());
            payload.extend_from_slice(&(row_bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(row_bytes);
            let _ = wal.append(tx_id, WalRecordType::Update, &payload);
        })?;
        Ok(result)
    }

    pub fn update(&mut self, table: &str, rid: RowId, values: &Row) -> io::Result<RowId> {
        // Mission B (post-review, second pass): WAL Off → no payload
        // construction.
        if self.wal.is_off() {
            return self.by_name_mut(table)?.update(rid, values);
        }
        let tbl = self.by_name_mut(table)?;
        let mut wal_bytes: Vec<u8> = Vec::new();
        encode_row_into(&tbl.schema, values, &mut wal_bytes);
        let tx_id = self.next_tx();
        self.wal_log(tx_id, WalRecordType::Update, table, rid, &wal_bytes)?;
        self.by_name_mut(table)?.update(rid, values)
    }

    /// Mission C Phase 2: update with a hint about which columns actually
    /// changed. Lets [`Table::update_hinted`] skip the old-row read when
    /// the hint shows no indexed column is in the changed set.
    pub fn update_hinted(
        &mut self,
        table: &str,
        rid: RowId,
        values: &Row,
        changed_col_indices: Option<&[usize]>,
    ) -> io::Result<RowId> {
        // Mission B (post-review, second pass): WAL Off → no payload
        // construction. The `update_by_filter` powql bench drives this
        // path tens of thousands of times per iteration.
        if self.wal.is_off() {
            return self
                .by_name_mut(table)?
                .update_hinted(rid, values, changed_col_indices);
        }
        let tbl = self.by_name_mut(table)?;
        let mut wal_bytes: Vec<u8> = Vec::new();
        encode_row_into(&tbl.schema, values, &mut wal_bytes);
        let tx_id = self.next_tx();
        self.wal_log(tx_id, WalRecordType::Update, table, rid, &wal_bytes)?;
        self.by_name_mut(table)?
            .update_hinted(rid, values, changed_col_indices)
    }

    /// Mission C Phase 4: fast-path update that patches a row's raw bytes
    /// in place, skipping decode/encode. Caller guarantees the mutation
    /// preserves the row length and touches no indexed column. Returns
    /// `Ok(true)` if the patch landed, `Ok(false)` if the row is gone.
    ///
    /// Mission B2: this primitive does NOT log to the WAL. Executor
    /// callers must route through [`Self::update_row_bytes_logged`] (or
    /// [`Self::update_row_bytes_logged_by_slot`]) so crash recovery
    /// sees the patched bytes. This raw form is retained for replay
    /// itself and any future callers that can tolerate the non-durable
    /// contract.
    #[inline]
    pub fn with_row_bytes_mut<F>(&mut self, table: &str, rid: RowId, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut [u8]),
    {
        self.by_name_mut(table)?.with_row_bytes_mut(rid, f)
    }

    /// Mission B2: WAL-logged variant of [`Self::with_row_bytes_mut`].
    /// Applies `f` to the live row bytes on the hot page, then reads
    /// the mutated bytes back and emits a `WalRecordType::Update`
    /// record so replay will re-apply the same patch after a crash.
    ///
    /// Ordering: the hot-page mutation happens first (in-memory only,
    /// no disk I/O), then the WAL record is appended and flushed. A
    /// crash after the mutation but before the WAL flush loses the
    /// update, but the caller never saw success in that case, so the
    /// contract holds: any `Ok(true)` return is durable.
    ///
    /// No hot-page eviction can happen between steps because this
    /// method holds the catalog's `&mut self` exclusively.
    #[inline]
    pub fn update_row_bytes_logged<F>(&mut self, table: &str, rid: RowId, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut [u8]),
    {
        let slot = *self.name_to_slot.get(table).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{table}' not found"),
            )
        })?;
        self.update_row_bytes_logged_by_slot(slot, rid, f)
    }

    /// Slot-indexed counterpart to [`Self::update_row_bytes_logged`].
    /// Used by prepared-query fast paths that already cached the table
    /// slot at prepare time and want to skip the name->slot probe on
    /// every execution.
    #[inline]
    pub fn update_row_bytes_logged_by_slot<F>(
        &mut self,
        slot: usize,
        rid: RowId,
        f: F,
    ) -> io::Result<bool>
    where
        F: FnOnce(&mut [u8]),
    {
        // Step 1: apply the mutation on the hot page. Failure here
        // (slot gone) short-circuits with Ok(false) — no WAL record.
        let tbl = &mut self.tables[slot];
        let ok = tbl.with_row_bytes_mut(rid, f)?;
        if !ok {
            return Ok(false);
        }
        // Mission B (post-review, second pass): in Off mode the per-row
        // get + clone + table-name clone + wal_log call are all wasted
        // — `wal.append` would no-op. Skip the snapshot path entirely.
        if self.wal.is_off() {
            return Ok(true);
        }
        // Step 2: snapshot the now-mutated bytes. `HeapFile::get`
        // observes the pinned hot page, so it returns the fresh row.
        let new_bytes = match tbl.heap.get(rid) {
            Some(b) => b,
            // Shouldn't happen — we just patched it — but be defensive.
            None => return Ok(false),
        };
        // Step 3: log + flush. Clone the table name out of the schema
        // so we can drop the `&mut tbl` borrow before touching `self.wal`.
        let table_name = tbl.schema.table_name.clone();
        let tx_id = self.next_tx();
        self.wal_log(tx_id, WalRecordType::Update, &table_name, rid, &new_bytes)?;
        Ok(true)
    }

    /// Mission C Phase 10: var-column in-place update fast path. Patches
    /// a single variable-length column's bytes directly into the row's
    /// slot, shrinking the row if the new value is smaller. Returns
    /// `Ok(false)` if the new value would grow the row (caller must fall
    /// back to the full encode path) or the row is gone.
    ///
    /// Caller guarantees no indexed column is touched — indexes are NOT
    /// maintained by this primitive.
    ///
    /// Mission B2: not WAL-logged. Executor callers should use
    /// [`Self::patch_var_col_logged`] instead.
    #[inline]
    pub fn patch_var_col_in_place(
        &mut self,
        table: &str,
        rid: RowId,
        col_idx: usize,
        new_value: Option<&[u8]>,
    ) -> io::Result<bool> {
        self.by_name_mut(table)?
            .patch_var_col_in_place(rid, col_idx, new_value)
    }

    /// Mission B2: WAL-logged variant of [`Self::patch_var_col_in_place`].
    /// Runs the in-place shrink on the hot page, then reads the mutated
    /// row bytes back and logs a `WalRecordType::Update` record. On a
    /// `false` return (grow-case bail) nothing is logged — the caller's
    /// fall-through to `update_hinted` handles the WAL itself.
    pub fn patch_var_col_logged(
        &mut self,
        table: &str,
        rid: RowId,
        col_idx: usize,
        new_value: Option<&[u8]>,
    ) -> io::Result<bool> {
        let slot = *self.name_to_slot.get(table).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{table}' not found"),
            )
        })?;
        let tbl = &mut self.tables[slot];
        let ok = tbl.patch_var_col_in_place(rid, col_idx, new_value)?;
        if !ok {
            return Ok(false);
        }
        // Mission B (post-review, second pass): WAL Off → skip the
        // snapshot + clone + log entirely.
        if self.wal.is_off() {
            return Ok(true);
        }
        let new_bytes = match tbl.heap.get(rid) {
            Some(b) => b,
            None => return Ok(false),
        };
        let table_name = tbl.schema.table_name.clone();
        let tx_id = self.next_tx();
        self.wal_log(tx_id, WalRecordType::Update, &table_name, rid, &new_bytes)?;
        Ok(true)
    }

    pub fn scan(&self, table: &str) -> io::Result<impl Iterator<Item = (RowId, Row)> + '_> {
        Ok(self.by_name(table)?.scan())
    }

    /// Zero-copy scan: passes raw row bytes to the callback without any
    /// per-row allocation. Used by the executor's fast paths.
    pub fn for_each_row_raw<F>(&self, table: &str, f: F) -> io::Result<()>
    where
        F: FnMut(RowId, &[u8]),
    {
        self.by_name(table)?.for_each_row_raw(f);
        Ok(())
    }

    /// Zero-copy scan with early termination. The callback returns
    /// `ControlFlow::Break(())` to stop. Used by `Limit` fast paths so a
    /// `limit 100` query doesn't pay decode/predicate cost for every row
    /// in the table after the limit is reached.
    pub fn try_for_each_row_raw<F>(&self, table: &str, f: F) -> io::Result<()>
    where
        F: FnMut(RowId, &[u8]) -> std::ops::ControlFlow<()>,
    {
        self.by_name(table)?.try_for_each_row_raw(f);
        Ok(())
    }

    pub fn create_index(&mut self, table: &str, column: &str) -> io::Result<()> {
        let data_dir = self.data_dir.clone();
        self.by_name_mut(table)?.create_index(column, &data_dir)?;
        // Mission 3: persist the updated catalog so the indexed column
        // list survives a restart. `Table::create_index` already saved
        // the btree file itself.
        self.persist()
    }

    pub fn index_lookup(&self, table: &str, column: &str, key: &Value) -> io::Result<Option<Row>> {
        Ok(self
            .by_name(table)?
            .index_lookup(column, key)
            .map(|(_, row)| row))
    }

    pub fn list_tables(&self) -> Vec<&str> {
        // Phase 18: iterate the Vec directly — schema.table_name is
        // the source of truth, and Vec order is insertion order (more
        // deterministic than the old FxHashMap keys).
        self.tables
            .iter()
            .map(|t| t.schema.table_name.as_str())
            .collect()
    }

    pub fn schema(&self, table: &str) -> Option<&Schema> {
        let slot = *self.name_to_slot.get(table)?;
        Some(&self.tables[slot].schema)
    }

    /// Drop a table: remove from the catalog and delete its data files.
    /// Returns `Err` if the table doesn't exist.
    // TODO(WAL): DDL is not replayed — track in follow-up
    pub fn drop_table(&mut self, name: &str) -> io::Result<()> {
        let slot = *self.name_to_slot.get(name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("table '{name}' not found"))
        })?;
        // Remove the data file.
        let table = &self.tables[slot];
        let heap_path = self
            .data_dir
            .join(format!("{}.heap", table.schema.table_name));
        if heap_path.exists() {
            fs::remove_file(&heap_path)?;
        }
        // Mission 3: remove only the .idx files that actually exist
        // (i.e. the columns the table currently has indexed). The pre-
        // Mission-3 code iterated every schema column blindly — harmless
        // but noisy. Now that we persist a real list of indexed columns,
        // we can be precise.
        for col_name in table.indexed_column_names() {
            let idx_path = self.data_dir.join(format!("{name}_{col_name}.idx"));
            if idx_path.exists() {
                let _ = fs::remove_file(&idx_path);
            }
        }
        // Swap-remove from the Vec and fix up name_to_slot.
        self.name_to_slot.remove(name);
        let last = self.tables.len() - 1;
        if slot != last {
            let moved_name = self.tables[last].schema.table_name.clone();
            self.tables.swap(slot, last);
            self.name_to_slot.insert(moved_name, slot);
        }
        self.tables.pop();
        self.persist()?;
        Ok(())
    }

    /// Add a column to an existing table's schema and backfill all
    /// existing rows to match the new shape.
    ///
    /// Older versions of this method only mutated the in-memory schema
    /// and relied on a (false) claim that "the heap format already
    /// handles short rows gracefully". It doesn't: `decode_row` reads
    /// exactly `n_var + 1` variable-column offsets from the row bytes
    /// using the CURRENT schema. Any row encoded with the old schema's
    /// (smaller) offset table would walk off the end of its buffer and
    /// panic with "range end index X out of range for slice of length Y"
    /// — which is exactly what a bare `Type` scan triggered right after
    /// an ALTER ADD COLUMN.
    ///
    /// The fix: rewrite every existing row through
    /// [`Table::rewrite_rows_for_schema_change`] so the on-disk
    /// encoding matches the new schema layout. Existing rows get
    /// `Value::Empty` for the new column.
    ///
    /// If the new column is `required` we refuse to add it to a
    /// non-empty table — there is no default value to backfill with,
    /// and silently storing `Empty` in a required slot would just
    /// shift the invariant violation to the next query.
    // TODO(WAL): DDL is not replayed — track in follow-up
    pub fn alter_table_add_column(&mut self, table: &str, col: ColumnDef) -> io::Result<()> {
        let data_dir = self.data_dir.clone();
        let tbl = self.by_name_mut(table)?;
        // Check for duplicate column name.
        if tbl.schema.columns.iter().any(|c| c.name == col.name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("column '{}' already exists in table '{table}'", col.name),
            ));
        }

        // Snapshot the old schema so we can decode existing rows with
        // the original layout before we mutate anything.
        let old_schema = tbl.schema.clone();

        // Peek at the heap to learn whether there are any existing
        // rows at all. An empty table is always safe to alter — no
        // rewrite needed, required columns are fine, etc.
        let has_rows = tbl.heap.scan().next().is_some();

        if has_rows && col.required {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot add required column '{}' to non-empty table '{table}': \
                     no default value to backfill existing rows with",
                    col.name
                ),
            ));
        }

        // Commit the new column into the schema and refresh the
        // cached layout so the rewrite below encodes with the new
        // shape.
        tbl.schema.columns.push(col);
        tbl.refresh_layout();

        if has_rows {
            // Build the "fill" template: all Empty, matching the new
            // schema width. `rewrite_rows_for_schema_change` will
            // overwrite old-column slots from each live row and leave
            // the new slot as Empty.
            let fill: Vec<Value> = vec![Value::Empty; tbl.schema.columns.len()];
            tbl.rewrite_rows_for_schema_change(&old_schema, &fill, &data_dir)?;
        }

        self.persist()?;
        Ok(())
    }

    /// Remove a column from an existing table's schema and rewrite
    /// every live row to match the new shape.
    ///
    /// Older versions of this method only mutated the in-memory schema
    /// and claimed that "reads simply won't decode the dropped column".
    /// That was wrong in several ways:
    ///
    ///   1. The null bitmap is indexed by column position. Dropping a
    ///      column shifts every later column's bit left, but old rows
    ///      still have bits in the original positions — so `is_null`
    ///      checks silently lie for every column after the dropped one.
    ///   2. The bitmap's byte width (`ceil(n_cols/8)`) can shrink when
    ///      `n_cols` crosses an 8-boundary, shifting every subsequent
    ///      byte of the row against the decoder's cursor.
    ///   3. Fixed-region size and the variable-offset-table width both
    ///      depend on the column set, so dropping any fixed or variable
    ///      column slides every following byte.
    ///
    /// The fix mirrors `alter_table_add_column`: snapshot the old
    /// schema, mutate to the new schema, then rewrite every row
    /// through [`Table::rewrite_rows_for_schema_change`]. Dropping a
    /// column from an empty table skips the rewrite.
    // TODO(WAL): DDL is not replayed — track in follow-up
    pub fn alter_table_drop_column(&mut self, table: &str, col_name: &str) -> io::Result<()> {
        let data_dir = self.data_dir.clone();
        let tbl = self.by_name_mut(table)?;
        let idx = tbl
            .schema
            .columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("column '{col_name}' not found in table '{table}'"),
                )
            })?;

        // Snapshot for decoding old rows.
        let old_schema = tbl.schema.clone();
        let has_rows = tbl.heap.scan().next().is_some();

        // Commit the schema change.
        tbl.schema.columns.remove(idx);
        for (i, col) in tbl.schema.columns.iter_mut().enumerate() {
            col.position = i as u16;
        }
        tbl.refresh_layout();

        if has_rows {
            // Build a filler matching the new (smaller) shape. The
            // rewrite path overwrites each new-column slot from the
            // matching old-column value by name, so the filler only
            // matters for brand-new columns — drop has none, so
            // `Empty` is a safe placeholder that never gets read.
            let fill: Vec<Value> = vec![Value::Empty; tbl.schema.columns.len()];
            tbl.rewrite_rows_for_schema_change(&old_schema, &fill, &data_dir)?;
        }

        self.persist()?;
        Ok(())
    }
}

impl Drop for Catalog {
    fn drop(&mut self) {
        // Mission 2: best-effort clean shutdown. `checkpoint` flushes
        // every heap and truncates the WAL, which is what
        // [`Catalog::open`] relies on to know that no replay is needed.
        //
        // We swallow errors here because Rust's `Drop` can't propagate
        // them and panicking during unwind is always a bigger problem
        // than a failed flush. The worst case on a failed drop-time
        // checkpoint is that the next open sees a non-empty WAL and
        // replays it (potentially producing duplicates — see the
        // [`Self::replay_wal`] caveat). That's strictly better than
        // losing committed writes.
        if let Err(e) = self.checkpoint() {
            warn!(error = %e, "catalog drop checkpoint failed");
        }
    }
}

// ─── WAL payload codec ─────────────────────────────────────────────────────
//
// Per-record payload layout (little-endian):
//
//   table_name_len : u32
//   table_name     : utf-8 bytes
//   page_id        : u32   (for insert: 0, ignored on replay)
//   slot_index     : u16   (for insert: 0, ignored on replay)
//   row_len        : u32
//   row_bytes      : raw encoded row (length = row_len)
//
// Lives next to `Catalog` because this is the only code that produces or
// consumes these records — the `Wal` itself is payload-agnostic.

fn encode_wal_payload(table: &str, rid: RowId, row_bytes: &[u8]) -> Vec<u8> {
    let name = table.as_bytes();
    let mut out = Vec::with_capacity(4 + name.len() + 4 + 2 + 4 + row_bytes.len());
    out.extend_from_slice(&(name.len() as u32).to_le_bytes());
    out.extend_from_slice(name);
    out.extend_from_slice(&rid.page_id.to_le_bytes());
    out.extend_from_slice(&rid.slot_index.to_le_bytes());
    out.extend_from_slice(&(row_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(row_bytes);
    out
}

fn decode_wal_payload(data: &[u8]) -> Option<(String, RowId, Vec<u8>)> {
    let mut pos = 0usize;
    if data.len() < 4 {
        return None;
    }
    let name_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;
    if pos + name_len > data.len() {
        return None;
    }
    let name = std::str::from_utf8(&data[pos..pos + name_len])
        .ok()?
        .to_string();
    pos += name_len;
    if pos + 4 + 2 + 4 > data.len() {
        return None;
    }
    let page_id = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?);
    pos += 4;
    let slot_index = u16::from_le_bytes(data[pos..pos + 2].try_into().ok()?);
    pos += 2;
    let row_len = u32::from_le_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
    pos += 4;
    if pos + row_len > data.len() {
        return None;
    }
    let row_bytes = data[pos..pos + row_len].to_vec();
    Some((
        name,
        RowId {
            page_id,
            slot_index,
        },
        row_bytes,
    ))
}

// ─── Catalog file format ────────────────────────────────────────────────────
//
// Layout (version 2):
//   magic     [4]      = "BCAT"
//   version   u16
//   n_tables  u32
//   for each table:
//     table_name_len  u32
//     table_name      utf8 bytes
//     n_columns       u16
//     for each column:
//       name_len      u32
//       name          utf8 bytes
//       type_id       u8
//       required      u8
//       position      u16
//     ── version 2 appends: ──
//     n_indexed_cols  u16
//     for each indexed column:
//       name_len      u32
//       name          utf8 bytes
//
// Version 1 files are accepted by the reader (same shape minus the
// trailing indexed-column block) and treated as having zero indexed
// columns. Writers always emit version 2 from Mission 3 onwards.

/// In-memory catalog entry pairing a schema with its indexed column list.
/// Produced by the reader; the writer takes the borrowed counterpart below.
pub(crate) struct CatalogEntry {
    pub schema: Schema,
    pub indexed_cols: Vec<String>,
}

/// Borrowed view passed to the writer.
pub(crate) struct CatalogEntryRef<'a> {
    pub schema: &'a Schema,
    pub indexed_cols: Vec<String>,
}

fn write_catalog_file(path: &Path, entries: &[CatalogEntryRef<'_>]) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    buf.extend_from_slice(CATALOG_MAGIC);
    buf.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    for entry in entries {
        let schema = entry.schema;
        let name = schema.table_name.as_bytes();
        buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
        buf.extend_from_slice(name);
        buf.extend_from_slice(&(schema.columns.len() as u16).to_le_bytes());
        for col in &schema.columns {
            let cn = col.name.as_bytes();
            buf.extend_from_slice(&(cn.len() as u32).to_le_bytes());
            buf.extend_from_slice(cn);
            buf.push(col.type_id as u8);
            buf.push(if col.required { 1 } else { 0 });
            buf.extend_from_slice(&col.position.to_le_bytes());
        }
        // Mission 3: per-table indexed column list (version 2 only).
        buf.extend_from_slice(&(entry.indexed_cols.len() as u16).to_le_bytes());
        for col_name in &entry.indexed_cols {
            let cn = col_name.as_bytes();
            buf.extend_from_slice(&(cn.len() as u32).to_le_bytes());
            buf.extend_from_slice(cn);
        }
    }

    let mut f = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    f.write_all(&buf)?;
    f.sync_data()?;
    Ok(())
}

fn read_catalog_file(path: &Path) -> io::Result<Vec<CatalogEntry>> {
    let mut f = fs::File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;

    let mut pos = 0usize;
    if buf.len() < 10 || &buf[0..4] != CATALOG_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad catalog magic",
        ));
    }
    pos += 4;
    let version = u16::from_le_bytes(buf[pos..pos + 2].try_into().unwrap());
    pos += 2;
    // Mission 3: accept version 1 files for forward compatibility.
    // `create_index` was the only mutator that added indexes before, and
    // those indexes were in-memory only, so on open we simply treat them
    // as absent and let the first `create_index` call repopulate the
    // metadata (and mint a version 2 file).
    if version != CATALOG_VERSION && version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported catalog version: {version}"),
        ));
    }
    let n_tables = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    let mut entries = Vec::with_capacity(n_tables);
    for _ in 0..n_tables {
        let name_len = read_u32(&buf, &mut pos)? as usize;
        let table_name = read_string(&buf, &mut pos, name_len)?;
        let n_cols = read_u16(&buf, &mut pos)? as usize;

        let mut columns = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            let cname_len = read_u32(&buf, &mut pos)? as usize;
            let name = read_string(&buf, &mut pos, cname_len)?;
            let type_id_raw = read_u8(&buf, &mut pos)?;
            let type_id = type_id_from_u8(type_id_raw)?;
            let required = read_u8(&buf, &mut pos)? != 0;
            let position = read_u16(&buf, &mut pos)?;
            columns.push(ColumnDef {
                name,
                type_id,
                required,
                position,
            });
        }

        // Version 2 appends the indexed column list. Version 1 stops
        // after the column block — default to an empty list.
        let indexed_cols = if version >= 2 {
            let n = read_u16(&buf, &mut pos)? as usize;
            let mut v = Vec::with_capacity(n);
            for _ in 0..n {
                let l = read_u32(&buf, &mut pos)? as usize;
                v.push(read_string(&buf, &mut pos, l)?);
            }
            v
        } else {
            Vec::new()
        };

        entries.push(CatalogEntry {
            schema: Schema {
                table_name,
                columns,
            },
            indexed_cols,
        });
    }

    Ok(entries)
}

fn read_u8(buf: &[u8], pos: &mut usize) -> io::Result<u8> {
    if *pos >= buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated catalog",
        ));
    }
    let v = buf[*pos];
    *pos += 1;
    Ok(v)
}
fn read_u16(buf: &[u8], pos: &mut usize) -> io::Result<u16> {
    if *pos + 2 > buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated catalog",
        ));
    }
    let v = u16::from_le_bytes(buf[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}
fn read_u32(buf: &[u8], pos: &mut usize) -> io::Result<u32> {
    if *pos + 4 > buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated catalog",
        ));
    }
    let v = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}
fn read_string(buf: &[u8], pos: &mut usize, len: usize) -> io::Result<String> {
    if *pos + len > buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated catalog string",
        ));
    }
    let s = std::str::from_utf8(&buf[*pos..*pos + len])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 in catalog"))?
        .to_string();
    *pos += len;
    Ok(s)
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
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown type id: {v}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn temp_catalog(name: &str) -> Catalog {
        let dir = std::env::temp_dir().join(format!("powdb_cat_{name}_{}", std::process::id()));
        Catalog::create(&dir).unwrap()
    }

    #[test]
    fn test_create_table_and_insert() {
        let mut cat = temp_catalog("basic");
        let schema = Schema {
            table_name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "name".into(),
                    type_id: TypeId::Str,
                    required: true,
                    position: 0,
                },
                ColumnDef {
                    name: "age".into(),
                    type_id: TypeId::Int,
                    required: false,
                    position: 1,
                },
            ],
        };
        cat.create_table(schema).unwrap();

        let row = vec![Value::Str("Alice".into()), Value::Int(30)];
        let rid = cat.insert("users", &row).unwrap();

        let result = cat.get("users", rid).unwrap();
        assert_eq!(result[0], Value::Str("Alice".into()));
        assert_eq!(result[1], Value::Int(30));
    }

    #[test]
    fn test_scan_table() {
        let mut cat = temp_catalog("scan");
        let schema = Schema {
            table_name: "items".into(),
            columns: vec![
                ColumnDef {
                    name: "name".into(),
                    type_id: TypeId::Str,
                    required: true,
                    position: 0,
                },
                ColumnDef {
                    name: "price".into(),
                    type_id: TypeId::Float,
                    required: true,
                    position: 1,
                },
            ],
        };
        cat.create_table(schema).unwrap();

        for i in 0..50 {
            cat.insert(
                "items",
                &vec![
                    Value::Str(format!("item_{i}")),
                    Value::Float(i as f64 * 1.5),
                ],
            )
            .unwrap();
        }

        let rows: Vec<_> = cat.scan("items").unwrap().collect();
        assert_eq!(rows.len(), 50);
    }

    #[test]
    fn test_index_lookup() {
        let mut cat = temp_catalog("idx");
        let schema = Schema {
            table_name: "users".into(),
            columns: vec![
                ColumnDef {
                    name: "email".into(),
                    type_id: TypeId::Str,
                    required: true,
                    position: 0,
                },
                ColumnDef {
                    name: "name".into(),
                    type_id: TypeId::Str,
                    required: true,
                    position: 1,
                },
            ],
        };
        cat.create_table(schema).unwrap();
        cat.create_index("users", "email").unwrap();

        cat.insert(
            "users",
            &vec![
                Value::Str("alice@example.com".into()),
                Value::Str("Alice".into()),
            ],
        )
        .unwrap();
        cat.insert(
            "users",
            &vec![
                Value::Str("bob@example.com".into()),
                Value::Str("Bob".into()),
            ],
        )
        .unwrap();

        let result = cat
            .index_lookup("users", "email", &Value::Str("bob@example.com".into()))
            .unwrap();
        assert!(result.is_some());
        let row = result.unwrap();
        assert_eq!(row[1], Value::Str("Bob".into()));
    }

    #[test]
    fn test_delete_row() {
        let mut cat = temp_catalog("delete");
        let schema = Schema {
            table_name: "t".into(),
            columns: vec![ColumnDef {
                name: "v".into(),
                type_id: TypeId::Int,
                required: true,
                position: 0,
            }],
        };
        cat.create_table(schema).unwrap();
        let r1 = cat.insert("t", &vec![Value::Int(1)]).unwrap();
        let r2 = cat.insert("t", &vec![Value::Int(2)]).unwrap();
        cat.delete("t", r1).unwrap();
        assert!(cat.get("t", r1).is_none());
        assert!(cat.get("t", r2).is_some());
    }

    #[test]
    fn test_update_row() {
        let mut cat = temp_catalog("update");
        let schema = Schema {
            table_name: "t".into(),
            columns: vec![ColumnDef {
                name: "v".into(),
                type_id: TypeId::Int,
                required: true,
                position: 0,
            }],
        };
        cat.create_table(schema).unwrap();
        let rid = cat.insert("t", &vec![Value::Int(1)]).unwrap();
        let new_rid = cat.update("t", rid, &vec![Value::Int(99)]).unwrap();
        let row = cat.get("t", new_rid).unwrap();
        assert_eq!(row[0], Value::Int(99));
    }

    #[test]
    fn test_persist_and_reopen() {
        let dir = std::env::temp_dir().join(format!("powdb_cat_persist_{}", std::process::id()));
        // Fresh dir
        let _ = std::fs::remove_dir_all(&dir);

        {
            let mut cat = Catalog::create(&dir).unwrap();
            cat.create_table(Schema {
                table_name: "users".into(),
                columns: vec![
                    ColumnDef {
                        name: "name".into(),
                        type_id: TypeId::Str,
                        required: true,
                        position: 0,
                    },
                    ColumnDef {
                        name: "age".into(),
                        type_id: TypeId::Int,
                        required: false,
                        position: 1,
                    },
                ],
            })
            .unwrap();
            cat.insert("users", &vec![Value::Str("Alice".into()), Value::Int(30)])
                .unwrap();
            cat.insert("users", &vec![Value::Str("Bob".into()), Value::Int(25)])
                .unwrap();
        }

        // Reopen — schema and rows should both still be there
        let cat = Catalog::open(&dir).unwrap();
        let schema = cat.schema("users").unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "name");
        assert_eq!(schema.columns[0].type_id, TypeId::Str);
        assert_eq!(schema.columns[1].type_id, TypeId::Int);

        let rows: Vec<_> = cat.scan("users").unwrap().collect();
        assert_eq!(rows.len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_open_missing_dir_errors() {
        let dir = std::env::temp_dir().join(format!("powdb_cat_missing_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // No catalog.bin yet
        assert!(Catalog::open(&dir).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_list_tables() {
        let mut cat = temp_catalog("list");
        cat.create_table(Schema {
            table_name: "a".into(),
            columns: vec![ColumnDef {
                name: "x".into(),
                type_id: TypeId::Int,
                required: true,
                position: 0,
            }],
        })
        .unwrap();
        cat.create_table(Schema {
            table_name: "b".into(),
            columns: vec![ColumnDef {
                name: "y".into(),
                type_id: TypeId::Int,
                required: true,
                position: 0,
            }],
        })
        .unwrap();
        let mut tables = cat.list_tables();
        tables.sort();
        assert_eq!(tables, vec!["a", "b"]);
    }
}
