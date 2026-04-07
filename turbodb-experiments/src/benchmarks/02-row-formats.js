/**
 * EXPERIMENT 2: Row Format Comparison
 * 
 * Questions:
 * - How much storage overhead does each format add per row?
 * - How does format affect write throughput?
 * - How does format affect full-scan throughput?
 * - How does format affect single-column scan throughput?
 * 
 * This isolates the FORMAT cost from the I/O cost.
 * All data stays in memory — no disk involved.
 */
import { PgHeapFormat, CompactRowFormat, ColumnarFormat } from '../formats/row-formats.js';
import { generateRows, benchmark, printTable, divider, formatBytes, timeOnce } from './utils.js';
import collector from '../results-collector.js';

const ROW_COUNTS = [1_000, 10_000, 100_000];

for (const rowCount of ROW_COUNTS) {
  divider(`EXPERIMENT 2: Row format comparison (${rowCount.toLocaleString()} rows)`);
  
  const rows = generateRows(rowCount);
  
  // ── Storage Overhead ──────────────────────────────────
  
  const pgHeap8k = new PgHeapFormat(8192);
  const pgHeap4k = new PgHeapFormat(4096);
  const compact4k = new CompactRowFormat(4096);
  const columnar4k = new ColumnarFormat(4096);
  
  const formats = [
    { label: 'pg-heap-8k', engine: pgHeap8k },
    { label: 'pg-heap-4k', engine: pgHeap4k },
    { label: 'compact-4k', engine: compact4k },
    { label: 'columnar-4k', engine: columnar4k },
  ];
  
  // Insert all rows into each format and measure
  const insertResults = formats.map(({ label, engine }) => {
    const { elapsedUs } = timeOnce(() => {
      for (const row of rows) engine.insertRow(row);
    });
    return { label, elapsedUs, engine };
  });
  
  // Storage stats comparison
  const storageStats = insertResults.map(({ label, engine }) => engine.getStorageStats());
  
  printTable('Storage overhead comparison', storageStats, [
    { label: 'Format', key: 'format' },
    { label: 'Rows', key: r => r.rows.toLocaleString() },
    { label: 'Pages', key: r => r.pages.toLocaleString() },
    { label: 'Total size', key: r => formatBytes(r.totalBytes) },
    { label: 'Data bytes', key: r => formatBytes(r.dataBytes) },
    { label: 'Overhead/row', key: r => `${r.overheadPerRow} B` },
    { label: 'Overhead %', key: r => `${r.overheadPct}%` },
  ]);
  
  // Insert throughput
  printTable('Insert throughput', insertResults.map(r => ({
    ...r,
    rowsPerSec: Math.round(rowCount / (r.elapsedUs / 1_000_000)),
  })), [
    { label: 'Format', key: 'label' },
    { label: 'Time (μs)', key: r => r.elapsedUs.toLocaleString() },
    { label: 'Rows/sec', key: r => r.rowsPerSec.toLocaleString() },
  ]);
  
  // ── Scan Performance ──────────────────────────────────
  
  if (rowCount >= 10_000) {
    // Full row scan
    const scanResults = formats.map(({ label, engine }) => {
      const result = benchmark(`${label} full scan`, () => {
        engine.scanAllRows();
      }, 100, 10);
      return { label, ...result };
    });
    
    printTable('Full row scan (all columns)', scanResults, [
      { label: 'Format', key: 'label' },
      { label: 'p50 (μs)', key: 'p50' },
      { label: 'p95 (μs)', key: 'p95' },
      { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
    ]);
    
    // Single column scan (this is where columnar should dominate)
    const colScanResults = formats.map(({ label, engine }) => {
      const result = benchmark(`${label} scan "age" only`, () => {
        engine.scanAllRows('age');
      }, 100, 10);
      return { label, ...result };
    });
    
    printTable('Single column scan ("age" only)', colScanResults, [
      { label: 'Format', key: 'label' },
      { label: 'p50 (μs)', key: 'p50' },
      { label: 'p95 (μs)', key: 'p95' },
      { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
    ]);
  }

  // ── Point Lookup ──────────────────────────────────────
  
  // For row stores, we need to scan to find (no index yet — that's the next experiment)
  // For columnar, it's an array index
  // This shows the raw format access cost without index overhead
  
  if (rowCount <= 10_000) {
    const locations = [];
    const pgHeapLookup = new PgHeapFormat(8192);
    for (const row of rows) {
      locations.push(pgHeapLookup.insertRow(row));
    }
    
    const lookupResult = benchmark('pg-heap direct tuple read', (i) => {
      const loc = locations[i % locations.length];
      pgHeapLookup.readRow(loc.page, loc.offset);
    }, 50_000, 1000);
    
    console.log(`  Direct tuple read (with known location):`);
    console.log(`    pg-heap: p50=${lookupResult.p50}μs  ops/s=${lookupResult.opsPerSec.toLocaleString()}`);
    
    const columnarLookup = new ColumnarFormat(4096);
    for (const row of rows) columnarLookup.insertRow(row);
    
    const colLookupResult = benchmark('columnar direct row read', (i) => {
      columnarLookup.readRow(i % rowCount);
    }, 50_000, 1000);
    
    console.log(`    columnar: p50=${colLookupResult.p50}μs  ops/s=${colLookupResult.opsPerSec.toLocaleString()}`);
    console.log();
  }
}

divider('Key takeaways');
console.log('  1. Compare overhead/row: pg-heap should be ~28B, compact ~2B, columnar ~8B');
console.log('  2. Full scan: row formats read all columns even if you need one.');
console.log('     Columnar should win on single-column scans by reading less data.');
console.log('  3. Point lookup: row formats can return a full row in one read.');
console.log('     Columnar needs to read from each column array separately.\n');

// Collect the 100K row results (last run)
collector.add('row_formats', { lastRunRowCount: ROW_COUNTS[ROW_COUNTS.length - 1] });
