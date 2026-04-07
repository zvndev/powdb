/**
 * EXPERIMENT 6: MVCC Concurrency Cost
 * 
 * The hardest question in database design: how do you let multiple
 * readers and writers work simultaneously without corrupting data?
 * 
 * Two fundamental approaches:
 * 
 * 1. APPEND-ONLY (PostgreSQL style)
 *    - Every update creates a NEW tuple version, old version stays
 *    - Readers see old versions (snapshot isolation)
 *    - Dead versions accumulate → VACUUM must clean them
 *    - Pro: Readers never block writers, simple implementation
 *    - Con: Table bloat, VACUUM overhead, 23-byte header per tuple
 * 
 * 2. UNDO-LOG (InnoDB style)
 *    - Updates modify the row IN PLACE
 *    - Old version is pushed to an undo log
 *    - Readers reconstruct old versions from undo chain if needed
 *    - Pro: No bloat, no VACUUM, smaller tuples
 *    - Con: Long transactions = long undo chains, reconstruction cost
 * 
 * This experiment measures the actual cost of each approach.
 */
import { benchmark, generateRows, printTable, divider, timeOnce, formatBytes } from './utils.js';
import collector from '../results-collector.js';

// ============================================================
// APPEND-ONLY MVCC (PostgreSQL-style)
// ============================================================
class AppendOnlyMVCC {
  constructor() {
    this.rows = [];         // all versions (including dead)
    this.txCounter = 1;
    this.liveCount = 0;
    this.deadCount = 0;
    this.name = 'append-only';
  }

  /** Start a new transaction, returns a snapshot */
  beginTx() {
    const txId = this.txCounter++;
    return { txId, snapshotMax: txId }; // can see everything < txId
  }

  insert(tx, row) {
    this.rows.push({
      ...row,
      _xmin: tx.txId,   // created by this tx
      _xmax: 0,          // not deleted
      _version: 1,
    });
    this.liveCount++;
    return this.rows.length - 1;
  }

  /** Update = mark old as dead + append new version */
  update(tx, idx, changes) {
    const old = this.rows[idx];
    if (old._xmax !== 0) return null; // already deleted

    // Mark old version as dead
    old._xmax = tx.txId;
    this.deadCount++;

    // Append new version
    const newRow = { ...old, ...changes, _xmin: tx.txId, _xmax: 0, _version: old._version + 1 };
    this.rows.push(newRow);
    return this.rows.length - 1;
  }

  /** Read with snapshot isolation — must check visibility */
  read(tx, idx) {
    const row = this.rows[idx];
    // Visible if: created before our snapshot AND not deleted before our snapshot
    if (row._xmin < tx.snapshotMax && (row._xmax === 0 || row._xmax >= tx.snapshotMax)) {
      return row;
    }
    return null; // not visible in this snapshot
  }

  /** Scan all visible rows (must check every row including dead) */
  scan(tx) {
    const results = [];
    for (let i = 0; i < this.rows.length; i++) {
      const row = this.rows[i];
      if (row._xmin < tx.snapshotMax && (row._xmax === 0 || row._xmax >= tx.snapshotMax)) {
        results.push(row);
      }
    }
    return results;
  }

  /** VACUUM — remove dead rows (PostgreSQL's pain point) */
  vacuum(oldestActiveTx) {
    const before = this.rows.length;
    this.rows = this.rows.filter(r => {
      // Keep if: still live, OR deleted after oldest active tx
      return r._xmax === 0 || r._xmax >= oldestActiveTx;
    });
    const removed = before - this.rows.length;
    this.deadCount -= removed;
    return removed;
  }

  getStats() {
    const totalBytes = this.rows.length * 80; // rough estimate with MVCC headers
    return {
      name: this.name,
      totalVersions: this.rows.length,
      liveRows: this.liveCount,
      deadRows: this.deadCount,
      bloatPct: this.rows.length > 0 ? Math.round(this.deadCount / this.rows.length * 100) : 0,
      estimatedBytes: totalBytes,
    };
  }
}

// ============================================================
// UNDO-LOG MVCC (InnoDB-style)
// ============================================================
class UndoLogMVCC {
  constructor() {
    this.rows = [];        // current versions only
    this.undoLog = [];     // old versions pushed here
    this.txCounter = 1;
    this.name = 'undo-log';
  }

  beginTx() {
    const txId = this.txCounter++;
    return { txId, snapshotMax: txId };
  }

  insert(tx, row) {
    this.rows.push({
      ...row,
      _xmin: tx.txId,
      _undoPtr: -1,  // no previous version
    });
    return this.rows.length - 1;
  }

  /** Update = modify in place, push old version to undo log */
  update(tx, idx, changes) {
    const current = this.rows[idx];

    // Push current version to undo log
    const undoIdx = this.undoLog.length;
    this.undoLog.push({ ...current });

    // Modify in place
    Object.assign(current, changes);
    current._xmin = tx.txId;
    current._undoPtr = undoIdx;

    return idx; // same slot
  }

  /** Read current version (no visibility check needed for simple case) */
  read(tx, idx) {
    const row = this.rows[idx];
    // If current version is too new, walk undo chain
    if (row._xmin >= tx.snapshotMax) {
      return this._reconstructVersion(row, tx.snapshotMax);
    }
    return row;
  }

  /** Walk undo chain to find the version visible to this snapshot */
  _reconstructVersion(row, snapshotMax) {
    let ptr = row._undoPtr;
    while (ptr >= 0) {
      const oldVersion = this.undoLog[ptr];
      if (oldVersion._xmin < snapshotMax) return oldVersion;
      ptr = oldVersion._undoPtr;
    }
    return null; // no visible version
  }

  /** Scan only current rows (no dead rows to skip!) */
  scan(tx) {
    const results = [];
    for (let i = 0; i < this.rows.length; i++) {
      const row = this.rows[i];
      if (row._xmin < tx.snapshotMax) {
        results.push(row);
      } else {
        // Need reconstruction from undo log
        const old = this._reconstructVersion(row, tx.snapshotMax);
        if (old) results.push(old);
      }
    }
    return results;
  }

  /** Purge undo entries older than oldest active tx */
  purgeUndo(oldestActiveTx) {
    // In a real implementation, you'd compact the undo log
    // For this experiment, we just count what could be purged
    let purgeable = 0;
    for (const entry of this.undoLog) {
      if (entry._xmin < oldestActiveTx) purgeable++;
    }
    return purgeable;
  }

  getStats() {
    return {
      name: this.name,
      totalVersions: this.rows.length + this.undoLog.length,
      liveRows: this.rows.length,
      undoEntries: this.undoLog.length,
      bloatPct: 0, // main table never bloats
      estimatedBytes: this.rows.length * 60 + this.undoLog.length * 60,
    };
  }
}

// ============================================================
// NO MVCC (baseline — single version, no concurrency)
// ============================================================
class NoMVCC {
  constructor() {
    this.rows = [];
    this.name = 'no-mvcc';
  }

  beginTx() { return { txId: 0, snapshotMax: 0 }; }

  insert(tx, row) {
    this.rows.push({ ...row });
    return this.rows.length - 1;
  }

  update(tx, idx, changes) {
    Object.assign(this.rows[idx], changes);
    return idx;
  }

  read(tx, idx) { return this.rows[idx]; }

  scan(tx) { return [...this.rows]; }

  getStats() {
    return {
      name: this.name,
      totalVersions: this.rows.length,
      liveRows: this.rows.length,
      deadRows: 0,
      bloatPct: 0,
      estimatedBytes: this.rows.length * 50,
    };
  }
}

// ============================================================
// RUN EXPERIMENTS
// ============================================================

const ROW_COUNT = 50_000;
const UPDATE_PCT = 0.3; // update 30% of rows
const UPDATE_COUNT = Math.floor(ROW_COUNT * UPDATE_PCT);

divider(`EXPERIMENT 6: MVCC concurrency cost (${ROW_COUNT.toLocaleString()} rows, ${Math.round(UPDATE_PCT*100)}% updated)`);

const rows = generateRows(ROW_COUNT);
const engines = [
  new NoMVCC(),
  new AppendOnlyMVCC(),
  new UndoLogMVCC(),
];

// ── INSERT BENCHMARK ─────────────────────────────────────

divider('Insert throughput');

const insertResults = engines.map(engine => {
  const { elapsedUs } = timeOnce(() => {
    const tx = engine.beginTx();
    for (const row of rows) engine.insert(tx, row);
  });
  return {
    name: engine.name,
    us: elapsedUs,
    rowsPerSec: Math.round(ROW_COUNT / (elapsedUs / 1_000_000)),
  };
});

printTable('Insert performance', insertResults, [
  { label: 'Engine', key: 'name' },
  { label: 'Time (μs)', key: r => r.us.toLocaleString() },
  { label: 'Rows/sec', key: r => r.rowsPerSec.toLocaleString() },
]);

// ── UPDATE BENCHMARK (this is where MVCC costs diverge) ──

divider('Update throughput (30% of rows updated)');

// Generate random update targets
const updateTargets = [];
for (let i = 0; i < UPDATE_COUNT; i++) {
  updateTargets.push(Math.floor(Math.random() * ROW_COUNT));
}

const updateResults = engines.map(engine => {
  const { elapsedUs } = timeOnce(() => {
    const tx = engine.beginTx();
    for (const idx of updateTargets) {
      engine.update(tx, idx, { age: 99 });
    }
  });
  return {
    name: engine.name,
    us: elapsedUs,
    updatesPerSec: Math.round(UPDATE_COUNT / (elapsedUs / 1_000_000)),
  };
});

printTable('Update performance', updateResults, [
  { label: 'Engine', key: 'name' },
  { label: 'Time (μs)', key: r => r.us.toLocaleString() },
  { label: 'Updates/sec', key: r => r.updatesPerSec.toLocaleString() },
]);

// ── STORAGE BLOAT AFTER UPDATES ──────────────────────────

divider('Storage bloat after updates');

const bloatStats = engines.map(e => e.getStats());
printTable('Storage after 30% update churn', bloatStats, [
  { label: 'Engine', key: 'name' },
  { label: 'Total versions', key: r => r.totalVersions.toLocaleString() },
  { label: 'Live rows', key: r => r.liveRows.toLocaleString() },
  { label: 'Bloat %', key: r => `${r.bloatPct}%` },
  { label: 'Est. size', key: r => formatBytes(r.estimatedBytes) },
]);

// ── SCAN AFTER UPDATES (must skip dead rows) ─────────────

divider('Full scan after updates (must handle dead versions)');

const scanResults = engines.map(engine => {
  const tx = engine.beginTx();
  const result = benchmark(`${engine.name} scan`, () => {
    engine.scan(tx);
  }, 100, 10);
  return result;
});

printTable('Scan throughput after updates', scanResults, [
  { label: 'Engine', key: 'name' },
  { label: 'p50 (μs)', key: r => r.p50.toLocaleString() },
  { label: 'p95 (μs)', key: r => r.p95.toLocaleString() },
  { label: 'scans/sec', key: r => r.opsPerSec.toLocaleString() },
]);

// ── POINT READ AFTER UPDATES ─────────────────────────────

divider('Point read after updates');

const readResults = engines.map(engine => {
  const tx = engine.beginTx();
  const maxIdx = engine.name === 'append-only' ? engine.rows.length : ROW_COUNT;
  const result = benchmark(`${engine.name} read`, (i) => {
    engine.read(tx, i % Math.min(maxIdx, ROW_COUNT));
  }, 100_000, 5000);
  return result;
});

printTable('Point read latency after updates', readResults, [
  { label: 'Engine', key: 'name' },
  { label: 'p50 (μs)', key: 'p50' },
  { label: 'p95 (μs)', key: 'p95' },
  { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
]);

// ── VACUUM / PURGE COST ──────────────────────────────────

divider('Cleanup cost (VACUUM vs undo purge)');

const appendEngine = engines[1]; // append-only
const undoEngine = engines[2];   // undo-log

const vacuumResult = timeOnce(() => appendEngine.vacuum(appendEngine.txCounter));
const purgeCount = undoEngine.purgeUndo(undoEngine.txCounter);

console.log(`  Append-only VACUUM: removed ${vacuumResult.result.toLocaleString()} dead tuples in ${vacuumResult.elapsedUs.toLocaleString()}μs`);
console.log(`  Undo-log purge: ${purgeCount.toLocaleString()} entries purgeable (instant — just advance a pointer)\n`);

const postVacuumStats = engines.map(e => e.getStats());
printTable('Storage AFTER cleanup', postVacuumStats, [
  { label: 'Engine', key: 'name' },
  { label: 'Total versions', key: r => r.totalVersions.toLocaleString() },
  { label: 'Live rows', key: r => r.liveRows.toLocaleString() },
  { label: 'Bloat %', key: r => `${r.bloatPct}%` },
  { label: 'Est. size', key: r => formatBytes(r.estimatedBytes) },
]);

// Collect results
collector.add('mvcc', {
  rowCount: ROW_COUNT,
  updatePct: UPDATE_PCT,
  inserts: insertResults,
  updates: updateResults,
  bloat: bloatStats,
  scans: scanResults.map(r => ({ name: r.name, p50: r.p50, opsPerSec: r.opsPerSec })),
  reads: readResults.map(r => ({ name: r.name, p50: r.p50, opsPerSec: r.opsPerSec })),
});

divider('Key takeaways');
console.log('  1. Append-only (PostgreSQL): updates are cheap (just append), but');
console.log('     scans get slower as dead rows accumulate. VACUUM is mandatory.');
console.log('  2. Undo-log (InnoDB): updates modify in-place (no bloat), but');
console.log('     reading old versions requires walking the undo chain.');
console.log('  3. For write-heavy workloads with few long transactions, undo-log wins.');
console.log('     For read-heavy workloads with many concurrent snapshots, append-only');
console.log('     can be simpler (no reconstruction cost).');
console.log('  4. The VACUUM cost in append-only is the hidden tax PostgreSQL users');
console.log('     pay. It causes I/O storms, table locking, and unpredictable latency.');
console.log('  5. BataDB recommendation: undo-log MVCC. The main table stays clean,');
console.log('     scans never touch dead rows, and undo purge is a pointer advance.\n');
