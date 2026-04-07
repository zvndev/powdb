import { performance } from 'node:perf_hooks';

/** Generate realistic test rows */
export function generateRows(count) {
  const firstNames = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Hank', 'Ivy', 'Jack',
    'Karen', 'Leo', 'Mona', 'Nick', 'Olivia', 'Paul', 'Quinn', 'Rosa', 'Sam', 'Tina'];
  const lastNames = ['Johnson', 'Smith', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
    'Rodriguez', 'Martinez', 'Anderson', 'Taylor', 'Thomas', 'Moore', 'Jackson'];
  const domains = ['gmail.com', 'outlook.com', 'company.io', 'example.org', 'mail.dev'];

  const rows = [];
  for (let i = 0; i < count; i++) {
    const first = firstNames[i % firstNames.length];
    const last = lastNames[Math.floor(i / firstNames.length) % lastNames.length];
    const domain = domains[i % domains.length];
    rows.push({
      id: i + 1,
      name: `${first} ${last}`,
      email: `${first.toLowerCase()}.${last.toLowerCase()}@${domain}`,
      age: 20 + (i % 50),
    });
  }
  return rows;
}

/** Run a function many times and collect timing stats */
export function benchmark(name, fn, iterations = 1000, warmup = 100) {
  // Warmup
  for (let i = 0; i < warmup; i++) fn(i);

  const times = new Float64Array(iterations);
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    fn(i);
    times[i] = (performance.now() - start) * 1000; // microseconds
  }

  times.sort();
  const sum = times.reduce((a, b) => a + b, 0);
  return {
    name,
    iterations,
    mean: Math.round(sum / iterations),
    p50: Math.round(times[Math.floor(iterations * 0.5)]),
    p95: Math.round(times[Math.floor(iterations * 0.95)]),
    p99: Math.round(times[Math.floor(iterations * 0.99)]),
    min: Math.round(times[0]),
    max: Math.round(times[iterations - 1]),
    totalMs: Math.round(sum / 1000),
    opsPerSec: Math.round(iterations / (sum / 1_000_000)),
  };
}

/** Time a single operation precisely */
export function timeOnce(fn) {
  const start = performance.now();
  const result = fn();
  const elapsed = (performance.now() - start) * 1000; // microseconds
  return { result, elapsedUs: Math.round(elapsed) };
}

/** Format bytes to human-readable */
export function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

/** Print a nice comparison table */
export function printTable(title, rows, columns) {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`  ${title}`);
  console.log('='.repeat(70));
  
  // Calculate column widths
  const widths = columns.map(col => Math.max(
    col.label.length,
    ...rows.map(r => String(typeof col.key === 'function' ? col.key(r) : r[col.key]).length)
  ));
  
  // Header
  const header = columns.map((col, i) => col.label.padEnd(widths[i])).join('  ');
  console.log(`  ${header}`);
  console.log(`  ${widths.map(w => '-'.repeat(w)).join('  ')}`);
  
  // Rows
  for (const row of rows) {
    const line = columns.map((col, i) => {
      const val = typeof col.key === 'function' ? col.key(row) : row[col.key];
      return String(val).padEnd(widths[i]);
    }).join('  ');
    console.log(`  ${line}`);
  }
  console.log();
}

/** Print a single benchmark result */
export function printBenchmark(result) {
  console.log(`  ${result.name}:`);
  console.log(`    p50=${result.p50}μs  p95=${result.p95}μs  p99=${result.p99}μs  ops/s=${result.opsPerSec.toLocaleString()}`);
}

export function divider(text) {
  console.log(`\n${'━'.repeat(70)}`);
  console.log(`  ${text}`);
  console.log('━'.repeat(70));
}
