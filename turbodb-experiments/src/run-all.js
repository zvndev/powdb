import fs from 'node:fs';
import collector from './results-collector.js';

console.log('╔══════════════════════════════════════════════════════════════════════╗');
console.log('║           TurboDB Storage Layer Experiments v0.3                    ║');
console.log('║  1. Raw page I/O    2. Row formats    3. SQLite comparison          ║');
console.log('║  4. Index perf      5. WAL cost       6. MVCC cost                 ║');
console.log('║  7. Vectorized execution                                            ║');
console.log('╚══════════════════════════════════════════════════════════════════════╝');
collector.print();

await import('./benchmarks/01-page-io.js');
await import('./benchmarks/02-row-formats.js');
await import('./benchmarks/03-sqlite-compare.js');
await import('./benchmarks/04-index-perf.js');
await import('./benchmarks/05-wal-cost.js');
await import('./benchmarks/06-mvcc-cost.js');
await import('./benchmarks/07-vectorized.js');

collector.save('./results.json');
console.log('\n=== RESULTS_JSON_START ===');
console.log(fs.readFileSync('./results.json', 'utf8'));
console.log('=== RESULTS_JSON_END ===\n');
