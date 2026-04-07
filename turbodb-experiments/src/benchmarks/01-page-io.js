/**
 * EXPERIMENT 1: Raw Page I/O
 * 
 * Question: What page size gives the best I/O throughput on this hardware?
 * 
 * PostgreSQL uses 8KB, InnoDB uses 16KB, SQLite uses 4KB.
 * LeanStore research showed 4KB is optimal for NVMe SSDs.
 * Let's measure it ourselves.
 */
import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { benchmark, printTable, divider, formatBytes } from './utils.js';
import collector from '../results-collector.js';

const DATA_DIR = '/home/claude/turbodb-experiments/data';
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

function benchPageWrites(pageSize, totalBytes) {
  const filepath = path.join(DATA_DIR, `test_${pageSize}.dat`);
  const numPages = Math.floor(totalBytes / pageSize);
  const buf = Buffer.alloc(pageSize);
  
  // Fill buffer with non-zero data (prevents OS zero-page optimization)
  for (let i = 0; i < pageSize; i++) buf[i] = i & 0xFF;
  
  const fd = fs.openSync(filepath, 'w');
  const start = performance.now();
  
  for (let i = 0; i < numPages; i++) {
    // Write a unique marker per page so the OS can't deduplicate
    buf.writeUInt32LE(i, 0);
    fs.writeSync(fd, buf, 0, pageSize, i * pageSize);
  }
  fs.fsyncSync(fd);
  
  const elapsed = performance.now() - start;
  fs.closeSync(fd);
  
  return {
    pageSize,
    numPages,
    totalBytes: numPages * pageSize,
    elapsedMs: Math.round(elapsed),
    throughputMBs: Math.round((numPages * pageSize) / (1024 * 1024) / (elapsed / 1000)),
    pagesPerSec: Math.round(numPages / (elapsed / 1000)),
    usPerPage: Math.round((elapsed * 1000) / numPages),
  };
}

function benchPageReads(pageSize, mode = 'sequential') {
  const filepath = path.join(DATA_DIR, `test_${pageSize}.dat`);
  if (!fs.existsSync(filepath)) return null;
  
  const stat = fs.statSync(filepath);
  const numPages = Math.floor(stat.size / pageSize);
  const buf = Buffer.alloc(pageSize);
  
  // Generate access pattern
  let offsets;
  if (mode === 'sequential') {
    offsets = Array.from({ length: numPages }, (_, i) => i);
  } else {
    // Random access — Fisher-Yates shuffle
    offsets = Array.from({ length: numPages }, (_, i) => i);
    for (let i = offsets.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [offsets[i], offsets[j]] = [offsets[j], offsets[i]];
    }
  }
  
  const fd = fs.openSync(filepath, 'r');
  const start = performance.now();
  
  for (let i = 0; i < numPages; i++) {
    fs.readSync(fd, buf, 0, pageSize, offsets[i] * pageSize);
  }
  
  const elapsed = performance.now() - start;
  fs.closeSync(fd);
  
  return {
    pageSize,
    mode,
    numPages,
    totalBytes: numPages * pageSize,
    elapsedMs: Math.round(elapsed),
    throughputMBs: Math.round((numPages * pageSize) / (1024 * 1024) / (elapsed / 1000)),
    pagesPerSec: Math.round(numPages / (elapsed / 1000)),
    usPerPage: Math.round((elapsed * 1000) / numPages),
  };
}

function benchLatency(pageSize, numSamples = 10000) {
  const filepath = path.join(DATA_DIR, `test_${pageSize}.dat`);
  if (!fs.existsSync(filepath)) return null;
  
  const stat = fs.statSync(filepath);
  const numPages = Math.floor(stat.size / pageSize);
  const buf = Buffer.alloc(pageSize);
  
  const fd = fs.openSync(filepath, 'r');
  
  // Measure individual read latencies for random pages
  const result = benchmark(`Random read ${formatBytes(pageSize)} pages`, (i) => {
    const pageNum = Math.floor(Math.random() * numPages);
    fs.readSync(fd, buf, 0, pageSize, pageNum * pageSize);
  }, numSamples, 500);
  
  fs.closeSync(fd);
  return result;
}

// ── Run the experiments ──────────────────────────────────────

divider('EXPERIMENT 1: Raw page I/O performance');
console.log('  Testing write/read throughput at different page sizes');
console.log('  This measures the raw hardware + OS capability before any database logic\n');

const TOTAL_DATA = 64 * 1024 * 1024; // 64 MB of data per test
const PAGE_SIZES = [512, 1024, 4096, 8192, 16384, 65536];

// Sequential writes
divider('Sequential write throughput');
const writeResults = PAGE_SIZES.map(ps => benchPageWrites(ps, TOTAL_DATA));
printTable('Write performance by page size', writeResults, [
  { label: 'Page size', key: r => formatBytes(r.pageSize) },
  { label: 'Pages', key: r => r.numPages.toLocaleString() },
  { label: 'Time (ms)', key: 'elapsedMs' },
  { label: 'MB/s', key: 'throughputMBs' },
  { label: 'Pages/s', key: r => r.pagesPerSec.toLocaleString() },
  { label: 'μs/page', key: 'usPerPage' },
]);

// Sequential reads
divider('Sequential read throughput');
const seqReadResults = PAGE_SIZES.map(ps => benchPageReads(ps, 'sequential'));
printTable('Sequential read performance by page size', seqReadResults.filter(Boolean), [
  { label: 'Page size', key: r => formatBytes(r.pageSize) },
  { label: 'Time (ms)', key: 'elapsedMs' },
  { label: 'MB/s', key: 'throughputMBs' },
  { label: 'Pages/s', key: r => r.pagesPerSec.toLocaleString() },
  { label: 'μs/page', key: 'usPerPage' },
]);

// Random reads (this is where SSDs vs HDDs diverge dramatically)
divider('Random read throughput');
const randReadResults = PAGE_SIZES.map(ps => benchPageReads(ps, 'random'));
printTable('Random read performance by page size', randReadResults.filter(Boolean), [
  { label: 'Page size', key: r => formatBytes(r.pageSize) },
  { label: 'Time (ms)', key: 'elapsedMs' },
  { label: 'MB/s', key: 'throughputMBs' },
  { label: 'Pages/s', key: r => r.pagesPerSec.toLocaleString() },
  { label: 'μs/page', key: 'usPerPage' },
]);

// Read latency distribution
divider('Random read latency distribution (per page)');
for (const ps of [4096, 8192, 16384]) {
  const lat = benchLatency(ps, 20000);
  if (lat) {
    console.log(`  ${formatBytes(ps)} pages: p50=${lat.p50}μs  p95=${lat.p95}μs  p99=${lat.p99}μs  ops/s=${lat.opsPerSec.toLocaleString()}`);
  }
}

// Cleanup
divider('Key takeaway');
console.log('  Compare sequential vs random read throughput.');
console.log('  On SSDs the gap should be small (2-5x). On HDDs it would be 100x+.');
console.log('  The "optimal" page size for your hardware is wherever pages/sec peaks');
console.log('  for random reads — that determines point lookup performance.\n');

// Collect results
collector.add('page_io', {
  writes: writeResults,
  sequentialReads: seqReadResults.filter(Boolean),
  randomReads: randReadResults.filter(Boolean),
});

// Cleanup test files
for (const ps of PAGE_SIZES) {
  const fp = path.join(DATA_DIR, `test_${ps}.dat`);
  if (fs.existsSync(fp)) fs.unlinkSync(fp);
}
