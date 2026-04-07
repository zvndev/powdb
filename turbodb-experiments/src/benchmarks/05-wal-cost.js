/**
 * EXPERIMENT 5: Durability Cost (WAL)
 * 
 * Questions:
 * - How much does fsync cost on this hardware?
 * - How much does group commit (batching) reduce that cost?
 * - What's the optimal batch size for throughput vs latency?
 * 
 * This is the experiment that separates "fast in-memory toy" from
 * "actual database." Every real database pays this cost. The question
 * is how much of it can be amortized.
 */
import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { WriteAheadLog } from '../storage/wal.js';
import { benchmark, printTable, divider, formatBytes, timeOnce } from './utils.js';
import collector from '../results-collector.js';

const DATA_DIR = '/home/claude/turbodb-experiments/data';
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// ── Raw fsync cost ──────────────────────────────────────

divider('EXPERIMENT 5: Durability cost');
console.log('  Measuring the price of crash safety.\n');

divider('Raw fsync latency');

const fsyncPath = path.join(DATA_DIR, 'fsync_test.dat');
const fsyncFd = fs.openSync(fsyncPath, 'w');
const smallBuf = Buffer.alloc(64);
smallBuf.writeUInt32LE(0xDEADBEEF, 0);

// Measure bare fsync cost
const fsyncResult = benchmark('fsync (no write)', () => {
  fs.fsyncSync(fsyncFd);
}, 2000, 200);

// Measure write + fsync cost
const writeFsyncResult = benchmark('write 64B + fsync', (i) => {
  smallBuf.writeUInt32LE(i, 4);
  fs.writeSync(fsyncFd, smallBuf, 0, 64, 0);
  fs.fsyncSync(fsyncFd);
}, 2000, 200);

// Measure just write (no fsync) for comparison
const writeOnlyResult = benchmark('write 64B (no fsync)', (i) => {
  smallBuf.writeUInt32LE(i, 4);
  fs.writeSync(fsyncFd, smallBuf, 0, 64, i * 64);
}, 10000, 1000);

fs.closeSync(fsyncFd);
fs.unlinkSync(fsyncPath);

printTable('Raw I/O latency', [writeOnlyResult, fsyncResult, writeFsyncResult], [
  { label: 'Operation', key: 'name' },
  { label: 'p50 (μs)', key: 'p50' },
  { label: 'p95 (μs)', key: 'p95' },
  { label: 'p99 (μs)', key: 'p99' },
  { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
]);

console.log('  fsync overhead = write+fsync minus write-only');
console.log(`  → fsync costs ~${writeFsyncResult.p50 - writeOnlyResult.p50}μs per call on this hardware\n`);

// ── WAL Mode Comparison ─────────────────────────────────

divider('WAL mode comparison (10,000 inserts)');

const RECORD_COUNT = 10_000;
const testRecord = Buffer.alloc(80); // typical row size
for (let i = 0; i < 80; i++) testRecord[i] = i & 0xFF;

const modes = [
  { mode: 'no_wal', label: 'No WAL (unsafe)' },
  { mode: 'wal_fsync', label: 'WAL + fsync per write' },
  { mode: 'wal_batch', label: 'WAL + batch (32 records)' },
];

const walResults = [];

for (const { mode, label } of modes) {
  const walPath = path.join(DATA_DIR, `wal_${mode}.log`);
  const wal = new WriteAheadLog(walPath, mode);
  wal.open();

  const { elapsedUs } = timeOnce(() => {
    for (let i = 0; i < RECORD_COUNT; i++) {
      testRecord.writeUInt32LE(i, 0);
      wal.appendRecord(testRecord);
    }
    wal.flush();
  });

  const stats = wal.getStats();
  wal.close();

  walResults.push({
    label,
    mode,
    elapsedUs,
    recordsPerSec: Math.round(RECORD_COUNT / (elapsedUs / 1_000_000)),
    fsyncs: stats.fsyncs,
    fsyncsPerWrite: stats.fsyncsPerWrite,
    bytesWritten: stats.bytesWritten,
  });

  // Cleanup
  if (fs.existsSync(walPath)) fs.unlinkSync(walPath);
}

printTable('WAL mode performance', walResults, [
  { label: 'Mode', key: 'label' },
  { label: 'Time (μs)', key: r => r.elapsedUs.toLocaleString() },
  { label: 'Records/sec', key: r => r.recordsPerSec.toLocaleString() },
  { label: 'fsyncs', key: r => r.fsyncs.toLocaleString() },
  { label: 'fsync/write', key: 'fsyncsPerWrite' },
  { label: 'vs no-WAL', key: r => `${(r.elapsedUs / walResults[0].elapsedUs).toFixed(1)}x slower` },
]);

// ── Batch Size Sweep ────────────────────────────────────

divider('Optimal batch size sweep');

const batchSizes = [1, 4, 8, 16, 32, 64, 128, 256, 512];
const batchResults = [];

for (const bs of batchSizes) {
  const walPath = path.join(DATA_DIR, `wal_batch_${bs}.log`);
  const wal = new WriteAheadLog(walPath, 'wal_batch');
  wal.batchSize = bs;
  wal.open();

  const { elapsedUs } = timeOnce(() => {
    for (let i = 0; i < RECORD_COUNT; i++) {
      testRecord.writeUInt32LE(i, 0);
      wal.appendRecord(testRecord);
    }
    wal.flush();
  });

  const stats = wal.getStats();
  wal.close();

  batchResults.push({
    batchSize: bs,
    elapsedUs,
    recordsPerSec: Math.round(RECORD_COUNT / (elapsedUs / 1_000_000)),
    fsyncs: stats.fsyncs,
    usPerRecord: Math.round(elapsedUs / RECORD_COUNT),
  });

  if (fs.existsSync(walPath)) fs.unlinkSync(walPath);
}

printTable('Batch size vs throughput', batchResults, [
  { label: 'Batch size', key: 'batchSize' },
  { label: 'Time (μs)', key: r => r.elapsedUs.toLocaleString() },
  { label: 'Records/sec', key: r => r.recordsPerSec.toLocaleString() },
  { label: 'fsyncs', key: r => r.fsyncs.toLocaleString() },
  { label: 'μs/record', key: 'usPerRecord' },
]);

// Find the knee of the curve
const noWalRate = walResults[0].recordsPerSec;
const bestBatch = batchResults.reduce((best, r) => r.recordsPerSec > best.recordsPerSec ? r : best);
console.log(`  Sweet spot: batch size ${bestBatch.batchSize} achieves ${bestBatch.recordsPerSec.toLocaleString()} records/sec`);
console.log(`  That's ${(bestBatch.recordsPerSec / noWalRate * 100).toFixed(0)}% of no-WAL throughput\n`);

// Collect results
collector.add('wal_durability', {
  rawFsyncUs: writeFsyncResult.p50 - writeOnlyResult.p50,
  walModes: walResults,
  batchSweep: batchResults,
  bestBatchSize: bestBatch.batchSize,
});

divider('Key takeaways');
console.log('  1. fsync is THE dominant cost for write-heavy workloads.');
console.log('     A single fsync can cost 100-2000μs depending on hardware.');
console.log('  2. Batch/group commit amortizes fsync across many writes.');
console.log('     TigerBeetle uses batch=32 and gets near-memory throughput.');
console.log('  3. The "no WAL" number is your theoretical write ceiling.');
console.log('     The gap between no-WAL and batched-WAL is the durability tax.');
console.log('  4. On NVMe SSDs, fsync is ~10-50μs. On SATA SSDs, ~100-500μs.');
console.log('     On HDDs, ~2000-10000μs. This is why hardware matters enormously');
console.log('     for write performance.\n');
