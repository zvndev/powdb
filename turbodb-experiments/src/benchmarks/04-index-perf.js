/**
 * EXPERIMENT 4: Index Performance
 * 
 * Questions:
 * - How does B-tree lookup compare to sequential scan at different table sizes?
 * - How does branching factor (order) affect B-tree performance?
 * - Hash index vs B-tree for point lookups?
 * - What's the cost of maintaining indexes during inserts?
 * - How does our B-tree compare to SQLite's built-in B-tree?
 */
import initSqlJs from 'sql.js';
import { BPlusTree, HashIndex } from '../storage/btree.js';
import { CompactRowFormat } from '../formats/row-formats.js';
import { generateRows, benchmark, printTable, divider } from './utils.js';
import collector from '../results-collector.js';

const ROW_COUNTS = [10_000, 100_000, 500_000];

for (const N of ROW_COUNTS) {
  divider(`EXPERIMENT 4: Index performance (${N.toLocaleString()} rows)`);
  
  const rows = generateRows(N);

  // ── Build indexes ───────────────────────────────────────
  
  // B-tree with different fan-out factors
  const btree64 = new BPlusTree(64);
  const btree256 = new BPlusTree(256);
  const btree1024 = new BPlusTree(1024);
  const hash = new HashIndex(N * 2); // 50% load factor
  
  // Also store in our compact format for scan comparison
  const store = new CompactRowFormat(4096);
  const locations = [];

  // Insert into all structures
  console.log('  Building indexes...');
  for (const row of rows) {
    const loc = store.insertRow(row);
    locations.push(loc);
    btree64.insert(row.id, loc);
    btree256.insert(row.id, loc);
    btree1024.insert(row.id, loc);
    hash.insert(row.id, loc);
  }

  // B-tree stats
  printTable('Index structure stats', [
    { label: 'B-tree (order=64)', height: btree64.getStats().height, order: 64 },
    { label: 'B-tree (order=256)', height: btree256.getStats().height, order: 256 },
    { label: 'B-tree (order=1024)', height: btree1024.getStats().height, order: 1024 },
    { label: 'Hash (buckets=' + (N*2).toLocaleString() + ')', height: 1, order: 'N/A',
      extra: 'avg chain: ' + hash.getStats().avgChainLength },
  ], [
    { label: 'Index', key: 'label' },
    { label: 'Height/depth', key: 'height' },
    { label: 'Order', key: 'order' },
  ]);

  // ── Point Lookup Benchmark ──────────────────────────────

  divider('Point lookup: index vs scan');

  // Sequential scan (worst case — no index)
  const scanLookup = benchmark('Sequential scan', (i) => {
    const targetId = (i % N) + 1;
    // Must scan all rows to find the one we want
    for (let j = 0; j < locations.length; j++) {
      const loc = locations[j];
      const row = store.readRow(loc.page, loc.offset);
      if (row.id === targetId) return row;
    }
  }, Math.min(500, N < 100_000 ? 1000 : 100), 10);

  // B-tree lookups at different orders
  const btree64Lookup = benchmark('B-tree (order=64)', (i) => {
    const loc = btree64.lookup((i % N) + 1);
    if (loc) store.readRow(loc.page, loc.offset);
  }, 100_000, 5000);

  const btree256Lookup = benchmark('B-tree (order=256)', (i) => {
    const loc = btree256.lookup((i % N) + 1);
    if (loc) store.readRow(loc.page, loc.offset);
  }, 100_000, 5000);

  const btree1024Lookup = benchmark('B-tree (order=1024)', (i) => {
    const loc = btree1024.lookup((i % N) + 1);
    if (loc) store.readRow(loc.page, loc.offset);
  }, 100_000, 5000);

  // Hash lookup
  const hashLookup = benchmark('Hash index', (i) => {
    const loc = hash.lookup((i % N) + 1);
    if (loc) store.readRow(loc.page, loc.offset);
  }, 100_000, 5000);

  // Direct location access (the theoretical minimum — you already know where it is)
  const directLookup = benchmark('Direct (known location)', (i) => {
    const loc = locations[i % N];
    store.readRow(loc.page, loc.offset);
  }, 100_000, 5000);

  // SQLite comparison
  const SQL = await initSqlJs();
  const db = new SQL.Database();
  db.run('PRAGMA journal_mode = MEMORY');
  db.run('PRAGMA synchronous = OFF');
  db.run(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER)`);
  db.run('BEGIN TRANSACTION');
  const ins = db.prepare('INSERT INTO users VALUES (?, ?, ?, ?)');
  for (const row of rows) { ins.bind([row.id, row.name, row.email, row.age]); ins.step(); ins.reset(); }
  ins.free();
  db.run('COMMIT');

  const sqliteStmt = db.prepare('SELECT name, email, age FROM users WHERE id = ?');
  const sqliteLookup = benchmark('SQLite B-tree', (i) => {
    sqliteStmt.bind([(i % N) + 1]); sqliteStmt.step(); sqliteStmt.getAsObject(); sqliteStmt.reset();
  }, 100_000, 5000);

  const allLookups = [scanLookup, btree64Lookup, btree256Lookup, btree1024Lookup, hashLookup, directLookup, sqliteLookup];

  printTable('Point lookup comparison', allLookups, [
    { label: 'Method', key: 'name' },
    { label: 'p50 (μs)', key: 'p50' },
    { label: 'p95 (μs)', key: 'p95' },
    { label: 'p99 (μs)', key: 'p99' },
    { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
    { label: 'vs scan', key: r => scanLookup.p50 > 0 ? `${Math.round(scanLookup.p50 / Math.max(r.p50, 0.1))}x` : 'N/A' },
  ]);

  // ── Range Scan Benchmark ────────────────────────────────
  
  if (N >= 100_000) {
    divider('Range scan: fetch 1000 rows by ID range');
    
    const rangeLow = Math.floor(N / 2);
    const rangeHigh = rangeLow + 999;

    const btreeRange = benchmark('B-tree range scan', () => {
      const locs = btree256.range(rangeLow, rangeHigh);
      for (const loc of locs) store.readRow(loc.page, loc.offset);
    }, 5000, 500);

    const sqliteRange = benchmark('SQLite range scan', () => {
      const s = db.prepare('SELECT * FROM users WHERE id BETWEEN ? AND ?');
      s.bind([rangeLow, rangeHigh]);
      while (s.step()) s.getAsObject();
      s.free();
    }, 5000, 500);

    printTable('Range scan (1000 rows)', [btreeRange, sqliteRange], [
      { label: 'Method', key: 'name' },
      { label: 'p50 (μs)', key: 'p50' },
      { label: 'p95 (μs)', key: 'p95' },
      { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
    ]);
  }

  // ── B-tree node visits ──────────────────────────────────
  
  btree256.resetStats();
  btree256.lookup(Math.floor(N / 2));
  const visited = btree256.getStats();
  console.log(`  B-tree (order=256) lookup internals for ${N.toLocaleString()} rows:`);
  console.log(`    Height: ${visited.height} | Nodes visited: ${visited.nodesVisited} | Comparisons: ${visited.comparisons}`);
  console.log(`    → With 8KB pages and 8-byte keys, this is ${visited.nodesVisited} page reads on disk\n`);

  sqliteStmt.free();
  db.close();
}

// Collect results (from last run — 500K rows)
collector.add('indexes', { note: 'see terminal output for full results' });

divider('Key takeaways');
console.log('  1. Sequential scan is O(n) — unusable for large tables.');
console.log('     B-tree is O(log n) — scales logarithmically.');
console.log('  2. Higher B-tree order = fewer levels = fewer page reads on disk.');
console.log('     Order 256 with 500K rows should be height 2-3 (2-3 disk reads).');
console.log('  3. Hash index should beat B-tree on point lookups (O(1) vs O(log n))');
console.log('     but cannot do range scans at all.');
console.log('  4. Our B-tree + compact format vs SQLite shows the overhead of');
console.log('     SQL parsing + query planning on top of the same tree structure.');
console.log('  5. "Direct known location" is the theoretical minimum — it is what');
console.log('     a compiled query with a known execution plan approaches.\n');
