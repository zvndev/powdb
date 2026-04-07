/**
 * EXPERIMENT 3: SQLite Comparison
 * 
 * The critical test: our hand-built storage formats vs a real, production database.
 * SQLite is the fairest comparison because it's also embedded (no network hop).
 * 
 * This isolates the database overhead layers:
 * - SQL parsing
 * - Query planning  
 * - B-tree traversal
 * - Type serialization
 * 
 * If our raw format is 5x faster than SQLite, we know those layers cost 80%.
 * If it's only 1.2x faster, the layers are cheap and not worth replacing.
 */
import initSqlJs from 'sql.js';
import fs from 'node:fs';
import path from 'node:path';
import { PgHeapFormat, CompactRowFormat, ColumnarFormat } from '../formats/row-formats.js';
import { generateRows, benchmark, printTable, divider, formatBytes, timeOnce } from './utils.js';
import collector from '../results-collector.js';

const DATA_DIR = '/home/claude/turbodb-experiments/data';
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const ROW_COUNT = 50_000;

divider(`EXPERIMENT 3: Our formats vs SQLite (${ROW_COUNT.toLocaleString()} rows)`);
console.log('  SQLite is embedded (WASM) — no network overhead. This is a pure');
console.log('  storage-engine-to-storage-engine comparison.\n');

const rows = generateRows(ROW_COUNT);

// ── Setup SQLite (WASM) ─────────────────────────────────

const SQL = await initSqlJs();
const db = new SQL.Database();
db.run('PRAGMA journal_mode = MEMORY');
db.run('PRAGMA synchronous = OFF');
db.run('PRAGMA cache_size = -64000');

db.run(`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    age INTEGER NOT NULL
  )
`);

// ── Setup our formats ───────────────────────────────────

const pgHeap = new PgHeapFormat(8192);
const compact = new CompactRowFormat(4096);
const columnar = new ColumnarFormat(4096);

// ── INSERT BENCHMARK ────────────────────────────────────

divider('Insert throughput');

// SQLite: individual inserts (no explicit transaction)
const sqliteInsertOne = timeOnce(() => {
  const stmt = db.prepare('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)');
  for (const row of rows) {
    stmt.bind([row.id, row.name, row.email, row.age]);
    stmt.step();
    stmt.reset();
  }
  stmt.free();
});

// SQLite: batched in transaction
const db2 = new SQL.Database();
db2.run('PRAGMA journal_mode = MEMORY');
db2.run('PRAGMA synchronous = OFF');
db2.run(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, age INTEGER NOT NULL)`);

const sqliteInsertBatch = timeOnce(() => {
  db2.run('BEGIN TRANSACTION');
  const stmt = db2.prepare('INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)');
  for (const row of rows) {
    stmt.bind([row.id, row.name, row.email, row.age]);
    stmt.step();
    stmt.reset();
  }
  stmt.free();
  db2.run('COMMIT');
});

const pgInsert = timeOnce(() => { for (const row of rows) pgHeap.insertRow(row); });
const compactInsert = timeOnce(() => { for (const row of rows) compact.insertRow(row); });
const columnarInsert = timeOnce(() => { for (const row of rows) columnar.insertRow(row); });

const insertResults = [
  { label: 'SQLite (no txn)', us: sqliteInsertOne.elapsedUs },
  { label: 'SQLite (batched txn)', us: sqliteInsertBatch.elapsedUs },
  { label: 'pg-heap (in-memory)', us: pgInsert.elapsedUs },
  { label: 'compact-row (in-memory)', us: compactInsert.elapsedUs },
  { label: 'columnar (in-memory)', us: columnarInsert.elapsedUs },
].map(r => ({ ...r, rowsPerSec: Math.round(ROW_COUNT / (r.us / 1_000_000)) }));

printTable('Insert performance', insertResults, [
  { label: 'Engine', key: 'label' },
  { label: 'Time (μs)', key: r => r.us.toLocaleString() },
  { label: 'Rows/sec', key: r => r.rowsPerSec.toLocaleString() },
  { label: 'vs SQLite batch', key: r => `${(sqliteInsertBatch.elapsedUs / r.us).toFixed(1)}x` },
]);

// Use db2 for all subsequent reads (it has the batched data)
const dbRead = db2;

// ── POINT LOOKUP BENCHMARK ──────────────────────────────

divider('Point lookup (by primary key)');

// SQLite: prepared statement lookup  
const sqliteLookupStmt = dbRead.prepare('SELECT name, email, age FROM users WHERE id = ?');
const sqliteLookup = benchmark('SQLite prepared lookup', (i) => {
  sqliteLookupStmt.bind([(i % ROW_COUNT) + 1]);
  sqliteLookupStmt.step();
  sqliteLookupStmt.getAsObject();
  sqliteLookupStmt.reset();
}, 100_000, 5000);

// SQLite: raw SQL (parse every time)
const sqliteRawLookup = benchmark('SQLite raw SQL lookup', (i) => {
  const s = dbRead.prepare('SELECT name, email, age FROM users WHERE id = ?');
  s.bind([(i % ROW_COUNT) + 1]);
  s.step();
  s.getAsObject();
  s.free();
}, 50_000, 1000);

// Our formats: direct access (we know the location)
// For a fair comparison, store locations during insert
const pgLocations = [];
const pgLookupEngine = new PgHeapFormat(8192);
for (const row of rows) pgLocations.push(pgLookupEngine.insertRow(row));

const pgLookup = benchmark('pg-heap direct read', (i) => {
  const loc = pgLocations[i % ROW_COUNT];
  pgLookupEngine.readRow(loc.page, loc.offset);
}, 100_000, 5000);

const compactLocations = [];
const compactLookupEngine = new CompactRowFormat(4096);
for (const row of rows) compactLocations.push(compactLookupEngine.insertRow(row));

const compactLookup = benchmark('compact direct read', (i) => {
  const loc = compactLocations[i % ROW_COUNT];
  compactLookupEngine.readRow(loc.page, loc.offset);
}, 100_000, 5000);

const columnarLookupEngine = new ColumnarFormat(4096);
for (const row of rows) columnarLookupEngine.insertRow(row);

const columnarLookup = benchmark('columnar direct read', (i) => {
  columnarLookupEngine.readRow(i % ROW_COUNT);
}, 100_000, 5000);

const lookupResults = [sqliteRawLookup, sqliteLookup, pgLookup, compactLookup, columnarLookup];

printTable('Point lookup latency', lookupResults, [
  { label: 'Method', key: 'name' },
  { label: 'p50 (μs)', key: 'p50' },
  { label: 'p95 (μs)', key: 'p95' },
  { label: 'p99 (μs)', key: 'p99' },
  { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
  { label: 'vs SQLite prep', key: r => `${(r.opsPerSec / sqliteLookup.opsPerSec).toFixed(1)}x` },
]);

// ── FULL SCAN BENCHMARK ─────────────────────────────────

divider('Full table scan (all columns)');

const sqliteScan = benchmark('SQLite SELECT *', () => {
  const results = dbRead.exec('SELECT * FROM users');
}, 50, 5);

const pgScan = benchmark('pg-heap scan', () => {
  pgLookupEngine.scanAllRows();
}, 50, 5);

const compactScan = benchmark('compact scan', () => {
  compactLookupEngine.scanAllRows();
}, 50, 5);

const columnarScan = benchmark('columnar scan', () => {
  columnarLookupEngine.scanAllRows();
}, 50, 5);

printTable('Full scan throughput', [sqliteScan, pgScan, compactScan, columnarScan], [
  { label: 'Method', key: 'name' },
  { label: 'p50 (μs)', key: r => r.p50.toLocaleString() },
  { label: 'p95 (μs)', key: r => r.p95.toLocaleString() },
  { label: 'scans/sec', key: r => r.opsPerSec.toLocaleString() },
  { label: 'rows/sec', key: r => (r.opsPerSec * ROW_COUNT).toLocaleString() },
]);

// ── SINGLE COLUMN SCAN ──────────────────────────────────

divider('Single column scan ("age" only)');

const sqliteColScan = benchmark('SQLite SELECT age', () => {
  dbRead.exec('SELECT age FROM users');
}, 50, 5);

const pgColScan = benchmark('pg-heap scan for age', () => {
  pgLookupEngine.scanAllRows('age');
}, 50, 5);

const compactColScan = benchmark('compact scan for age', () => {
  compactLookupEngine.scanAllRows('age');
}, 50, 5);

const columnarColScan = benchmark('columnar scan for age', () => {
  columnarLookupEngine.scanAllRows('age');
}, 50, 5);

printTable('Single column scan', [sqliteColScan, pgColScan, compactColScan, columnarColScan], [
  { label: 'Method', key: 'name' },
  { label: 'p50 (μs)', key: r => r.p50.toLocaleString() },
  { label: 'scans/sec', key: r => r.opsPerSec.toLocaleString() },
  { label: 'vs SQLite', key: r => `${(r.opsPerSec / sqliteColScan.opsPerSec).toFixed(1)}x` },
]);

// ── AGGREGATION ─────────────────────────────────────────

divider('Aggregation (AVG age)');

const sqliteAgg = benchmark('SQLite AVG(age)', () => {
  dbRead.exec('SELECT AVG(age) FROM users');
}, 200, 20);

const columnarAgg = benchmark('columnar AVG(age)', () => {
  const ages = columnarLookupEngine.readColumn('age');
  let sum = 0;
  for (let i = 0; i < ages.length; i++) sum += ages[i];
  return sum / ages.length;
}, 200, 20);

const pgAgg = benchmark('pg-heap AVG(age)', () => {
  const ages = pgLookupEngine.scanAllRows('age');
  let sum = 0;
  for (let i = 0; i < ages.length; i++) sum += ages[i];
  return sum / ages.length;
}, 200, 20);

printTable('Aggregation', [sqliteAgg, pgAgg, columnarAgg], [
  { label: 'Method', key: 'name' },
  { label: 'p50 (μs)', key: r => r.p50.toLocaleString() },
  { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
  { label: 'vs SQLite', key: r => `${(r.opsPerSec / sqliteAgg.opsPerSec).toFixed(1)}x` },
]);

// ── STORAGE EFFICIENCY ──────────────────────────────────

divider('Storage efficiency comparison');

const sqliteExport = dbRead.export();
const sqliteSize = sqliteExport.length;
const pgStats = pgLookupEngine.getStorageStats();
const compactStats = compactLookupEngine.getStorageStats();
const columnarStats = columnarLookupEngine.getStorageStats();

const effResults = [
  { label: 'SQLite', size: sqliteSize, overhead: 'N/A (includes B-tree + WAL)' },
  { label: 'pg-heap-8k', size: pgStats.totalBytes, overhead: `${pgStats.overheadPerRow} B/row (${pgStats.overheadPct}%)` },
  { label: 'compact-4k', size: compactStats.totalBytes, overhead: `${compactStats.overheadPerRow} B/row (${compactStats.overheadPct}%)` },
  { label: 'columnar-4k', size: columnarStats.totalBytes, overhead: `${columnarStats.overheadPerRow} B/row (${columnarStats.overheadPct}%)` },
];

printTable('Storage size', effResults, [
  { label: 'Format', key: 'label' },
  { label: 'Total size', key: r => formatBytes(r.size) },
  { label: 'Bytes/row', key: r => Math.round(r.size / ROW_COUNT) },
  { label: 'Overhead', key: 'overhead' },
  { label: 'vs SQLite', key: r => `${(r.size / sqliteSize).toFixed(2)}x` },
]);

// Cleanup
sqliteLookupStmt.free();
dbRead.close();

// Collect all results for export
collector.add('sqlite_compare', {
  rowCount: ROW_COUNT,
  inserts: {
    sqliteNoTxn: { us: sqliteInsertOne.elapsedUs },
    sqliteBatchTxn: { us: sqliteInsertBatch.elapsedUs },
    pgHeap: { us: pgInsert.elapsedUs },
    compact: { us: compactInsert.elapsedUs },
    columnar: { us: columnarInsert.elapsedUs },
  },
  pointLookup: {
    sqliteRawSQL: sqliteRawLookup,
    sqlitePrepared: sqliteLookup,
    pgHeapDirect: pgLookup,
    compactDirect: compactLookup,
    columnarDirect: columnarLookup,
  },
  fullScan: {
    sqlite: sqliteScan,
    pgHeap: pgScan,
    compact: compactScan,
    columnar: columnarScan,
  },
  singleColumnScan: {
    sqlite: sqliteColScan,
    pgHeap: pgColScan,
    compact: compactColScan,
    columnar: columnarColScan,
  },
  aggregation: {
    sqlite: sqliteAgg,
    pgHeap: pgAgg,
    columnar: columnarAgg,
  },
  storage: {
    sqliteBytes: sqliteSize,
    pgHeap: pgStats,
    compact: compactStats,
    columnar: columnarStats,
  },
});

divider('What these numbers tell us');
console.log('  1. SQLite "raw SQL" vs "prepared" gap = cost of parsing SQL text');
console.log('  2. SQLite "prepared" vs our "direct read" gap = cost of B-tree + planning');
console.log('  3. Columnar vs row-store gap on single-column scan = cost of reading');
console.log('     unnecessary columns (the "scan amplification" problem)');
console.log('  4. If our in-memory formats are >5x faster than SQLite,');
console.log('     the database layers (parse/plan/execute) dominate latency.');
console.log('  5. If they are only 1-2x faster, the I/O and format are the real cost,');
console.log('     and optimizing the query layers won\'t help much.\n');
