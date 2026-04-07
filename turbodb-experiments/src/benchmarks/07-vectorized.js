/**
 * EXPERIMENT 7: Vectorized Execution
 * 
 * The query execution model determines how data flows through operators
 * (filter, project, aggregate, join). Three approaches:
 * 
 * 1. VOLCANO (tuple-at-a-time) — PostgreSQL, SQLite
 *    Each operator calls next() and gets ONE row. Simple but slow.
 *    Virtual function calls per row, terrible cache utilization.
 * 
 * 2. VECTORIZED (batch-at-a-time) — DuckDB, ClickHouse
 *    Each operator processes a BATCH of 1024+ values.
 *    Tight loops, cache-friendly, SIMD-exploitable.
 * 
 * 3. COMPILED (whole-query) — Umbra, HyPer
 *    The entire query is JIT-compiled into a single tight loop.
 *    No operator boundaries at runtime.
 * 
 * This experiment builds a minimal vectorized executor and compares
 * it against tuple-at-a-time processing on the same data.
 */
import { generateRows, benchmark, printTable, divider, timeOnce } from './utils.js';
import collector from '../results-collector.js';

const ROW_COUNT = 500_000;

divider(`EXPERIMENT 7: Vectorized execution (${ROW_COUNT.toLocaleString()} rows)`);

// ── Prepare columnar data ────────────────────────────────

console.log('  Preparing data...');
const rows = generateRows(ROW_COUNT);

// Row-oriented storage (array of objects — what ORMs return)
const rowStore = rows;

// Columnar storage (struct-of-arrays — what vectorized engines use)
const colStore = {
  id:    new Int32Array(ROW_COUNT),
  age:   new Int16Array(ROW_COUNT),
  name:  new Array(ROW_COUNT),
  email: new Array(ROW_COUNT),
};
for (let i = 0; i < ROW_COUNT; i++) {
  colStore.id[i] = rows[i].id;
  colStore.age[i] = rows[i].age;
  colStore.name[i] = rows[i].name;
  colStore.email[i] = rows[i].email;
}

const BATCH_SIZE = 2048; // DuckDB uses 2048

// ============================================================
// QUERY 1: Filter + Count
// SELECT COUNT(*) FROM users WHERE age > 30
// ============================================================

divider('Query 1: SELECT COUNT(*) WHERE age > 30');

// Volcano style: iterate one row at a time
const volcanoFilter = benchmark('Volcano (row-at-a-time)', () => {
  let count = 0;
  for (let i = 0; i < rowStore.length; i++) {
    if (rowStore[i].age > 30) count++;
  }
  return count;
}, 500, 50);

// Vectorized: process typed arrays in batches
const vectorizedFilter = benchmark('Vectorized (batch)', () => {
  let count = 0;
  const ages = colStore.age;
  const len = ages.length;
  for (let i = 0; i < len; i++) {
    if (ages[i] > 30) count++;
  }
  return count;
}, 500, 50);

// Vectorized with manual unrolling (mimics SIMD-style processing)
const vectorizedUnrolled = benchmark('Vectorized (unrolled 4x)', () => {
  let count = 0;
  const ages = colStore.age;
  const len = ages.length;
  const limit = len - (len % 4);
  let c0 = 0, c1 = 0, c2 = 0, c3 = 0;
  for (let i = 0; i < limit; i += 4) {
    if (ages[i]     > 30) c0++;
    if (ages[i + 1] > 30) c1++;
    if (ages[i + 2] > 30) c2++;
    if (ages[i + 3] > 30) c3++;
  }
  count = c0 + c1 + c2 + c3;
  for (let i = limit; i < len; i++) {
    if (ages[i] > 30) count++;
  }
  return count;
}, 500, 50);

printTable('Filter + Count performance', [volcanoFilter, vectorizedFilter, vectorizedUnrolled], [
  { label: 'Method', key: 'name' },
  { label: 'p50 (μs)', key: r => r.p50.toLocaleString() },
  { label: 'p95 (μs)', key: r => r.p95.toLocaleString() },
  { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
  { label: 'rows/sec', key: r => (r.opsPerSec * ROW_COUNT).toLocaleString() },
  { label: 'vs Volcano', key: r => volcanoFilter.p50 > 0 ? `${(volcanoFilter.p50 / Math.max(r.p50, 0.1)).toFixed(1)}x` : 'N/A' },
]);

// ============================================================
// QUERY 2: Filter + Project + Aggregate
// SELECT AVG(age) FROM users WHERE age > 30
// ============================================================

divider('Query 2: SELECT AVG(age) WHERE age > 30');

const volcanoAvg = benchmark('Volcano (row-at-a-time)', () => {
  let sum = 0, count = 0;
  for (let i = 0; i < rowStore.length; i++) {
    if (rowStore[i].age > 30) { sum += rowStore[i].age; count++; }
  }
  return sum / count;
}, 500, 50);

const vectorizedAvg = benchmark('Vectorized (typed array)', () => {
  let sum = 0, count = 0;
  const ages = colStore.age;
  const len = ages.length;
  for (let i = 0; i < len; i++) {
    if (ages[i] > 30) { sum += ages[i]; count++; }
  }
  return sum / count;
}, 500, 50);

printTable('Filter + Avg performance', [volcanoAvg, vectorizedAvg], [
  { label: 'Method', key: 'name' },
  { label: 'p50 (μs)', key: r => r.p50.toLocaleString() },
  { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
  { label: 'vs Volcano', key: r => volcanoAvg.p50 > 0 ? `${(volcanoAvg.p50 / Math.max(r.p50, 0.1)).toFixed(1)}x` : 'N/A' },
]);

// ============================================================
// QUERY 3: Group-by aggregation
// SELECT age, COUNT(*) FROM users GROUP BY age
// ============================================================

divider('Query 3: SELECT age, COUNT(*) GROUP BY age');

const volcanoGroupBy = benchmark('Volcano (row-at-a-time)', () => {
  const groups = new Map();
  for (let i = 0; i < rowStore.length; i++) {
    const age = rowStore[i].age;
    groups.set(age, (groups.get(age) || 0) + 1);
  }
  return groups.size;
}, 200, 20);

const vectorizedGroupBy = benchmark('Vectorized (typed array)', () => {
  // Use a flat array since ages are bounded (0-100)
  const counts = new Int32Array(101);
  const ages = colStore.age;
  const len = ages.length;
  for (let i = 0; i < len; i++) {
    counts[ages[i]]++;
  }
  let groups = 0;
  for (let i = 0; i < 101; i++) if (counts[i] > 0) groups++;
  return groups;
}, 200, 20);

printTable('Group-by performance', [volcanoGroupBy, vectorizedGroupBy], [
  { label: 'Method', key: 'name' },
  { label: 'p50 (μs)', key: r => r.p50.toLocaleString() },
  { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
  { label: 'vs Volcano', key: r => volcanoGroupBy.p50 > 0 ? `${(volcanoGroupBy.p50 / Math.max(r.p50, 0.1)).toFixed(1)}x` : 'N/A' },
]);

// ============================================================
// QUERY 4: Multi-column filter + project (tests cache effects)
// SELECT name, email FROM users WHERE age > 30 AND id < 100000
// ============================================================

divider('Query 4: Multi-column filter + project');

const volcanoMulti = benchmark('Volcano (row objects)', () => {
  const results = [];
  for (let i = 0; i < rowStore.length; i++) {
    const r = rowStore[i];
    if (r.age > 30 && r.id < 100000) {
      results.push({ name: r.name, email: r.email });
    }
  }
  return results.length;
}, 100, 10);

const vectorizedMulti = benchmark('Vectorized (columnar filter)', () => {
  // First pass: build selection vector from typed arrays
  const selectionVector = new Uint32Array(ROW_COUNT);
  let selCount = 0;
  const ages = colStore.age;
  const ids = colStore.id;
  const len = ages.length;
  for (let i = 0; i < len; i++) {
    if (ages[i] > 30 && ids[i] < 100000) {
      selectionVector[selCount++] = i;
    }
  }
  // Second pass: gather matching names and emails using selection vector
  const names = new Array(selCount);
  const emails = new Array(selCount);
  for (let j = 0; j < selCount; j++) {
    const idx = selectionVector[j];
    names[j] = colStore.name[idx];
    emails[j] = colStore.email[idx];
  }
  return selCount;
}, 100, 10);

printTable('Multi-column filter + project', [volcanoMulti, vectorizedMulti], [
  { label: 'Method', key: 'name' },
  { label: 'p50 (μs)', key: r => r.p50.toLocaleString() },
  { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
  { label: 'vs Volcano', key: r => volcanoMulti.p50 > 0 ? `${(volcanoMulti.p50 / Math.max(r.p50, 0.1)).toFixed(1)}x` : 'N/A' },
]);

// ============================================================
// DATA FORMAT OVERHEAD: object vs typed array
// ============================================================

divider('Data format overhead: JS objects vs typed arrays');

// Measure just the iteration cost — no computation
const iterObjects = benchmark('Iterate row objects', () => {
  let x = 0;
  for (let i = 0; i < rowStore.length; i++) x += rowStore[i].age;
  return x;
}, 200, 20);

const iterTypedArray = benchmark('Iterate Int16Array', () => {
  let x = 0;
  const ages = colStore.age;
  for (let i = 0; i < ages.length; i++) x += ages[i];
  return x;
}, 200, 20);

printTable('Raw iteration cost (just summing age)', [iterObjects, iterTypedArray], [
  { label: 'Method', key: 'name' },
  { label: 'p50 (μs)', key: r => r.p50.toLocaleString() },
  { label: 'ops/sec', key: r => r.opsPerSec.toLocaleString() },
  { label: 'vs objects', key: r => iterObjects.p50 > 0 ? `${(iterObjects.p50 / Math.max(r.p50, 0.1)).toFixed(1)}x` : 'N/A' },
]);

// Collect results
collector.add('vectorized_execution', {
  rowCount: ROW_COUNT,
  filterCount: { volcano: volcanoFilter, vectorized: vectorizedFilter, unrolled: vectorizedUnrolled },
  filterAvg: { volcano: volcanoAvg, vectorized: vectorizedAvg },
  groupBy: { volcano: volcanoGroupBy, vectorized: vectorizedGroupBy },
  multiColumn: { volcano: volcanoMulti, vectorized: vectorizedMulti },
  iteration: { objects: iterObjects, typedArray: iterTypedArray },
});

divider('Key takeaways');
console.log('  1. Typed arrays (Int16Array, Int32Array) vs JS objects shows the raw');
console.log('     cost of object property access vs contiguous memory iteration.');
console.log('  2. The vectorized advantage comes from:');
console.log('     - Cache locality (contiguous memory vs pointer-chasing)');
console.log('     - No object property lookup overhead');
console.log('     - CPU branch prediction (tight loop vs virtual dispatch)');
console.log('     - Potential for SIMD in native implementations');
console.log('  3. Group-by with bounded keys (age 0-100) uses a flat array instead');
console.log('     of a hash map — this is a classic vectorized optimization.');
console.log('  4. The selection vector pattern (filter → gather) is how DuckDB');
console.log('     processes complex multi-column predicates efficiently.');
console.log('  5. In native code (Rust/Zig/C) with actual SIMD, the vectorized');
console.log('     advantage would be 10-50x larger than what JS can show.\n');
