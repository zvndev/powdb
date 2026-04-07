import http from 'node:http';
import fs from 'node:fs';
import collector from './results-collector.js';

const PORT = process.env.PORT || 3000;
let status = 'running';
let resultsJson = null;
let logOutput = '';

// Capture console.log output
const origLog = console.log;
console.log = (...args) => {
  const line = args.join(' ');
  logOutput += line + '\n';
  origLog.apply(console, args);
};

// Start the server immediately so Railway sees a healthy service
const server = http.createServer((req, res) => {
  if (req.url === '/results.json' && resultsJson) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(resultsJson);
  } else if (req.url === '/logs') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end(logOutput);
  } else {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(`<!DOCTYPE html><html><body style="font-family:monospace;max-width:800px;margin:40px auto;padding:0 20px">
      <h2>TurboDB Storage Experiments</h2>
      <p>Status: <strong>${status}</strong></p>
      ${status === 'done'
        ? '<p><a href="/results.json">Download results.json</a> — paste this back to Claude</p><h3>Logs</h3><pre style="background:#111;color:#0f0;padding:16px;overflow:auto;font-size:12px;max-height:80vh">' + logOutput.replace(/</g,'&lt;') + '</pre>'
        : '<p>Benchmarks are running (~3-5 minutes). Refresh this page to check progress.</p><h3>Live output</h3><pre style="background:#111;color:#0f0;padding:16px;overflow:auto;font-size:12px;max-height:80vh">' + logOutput.replace(/</g,'&lt;') + '</pre><script>setTimeout(()=>location.reload(),5000)</script>'
      }
    </body></html>`);
  }
});

server.listen(PORT, () => {
  origLog(`Server running on port ${PORT}`);
  origLog('Benchmarks starting...\n');
  runBenchmarks();
});

async function runBenchmarks() {
  try {
    collector.print();
    await import('./benchmarks/01-page-io.js');
    console.log('\n');
    await import('./benchmarks/02-row-formats.js');
    console.log('\n');
    await import('./benchmarks/03-sqlite-compare.js');
    console.log('\n');
    await import('./benchmarks/04-index-perf.js');
    console.log('\n');
    await import('./benchmarks/05-wal-cost.js');
    await import('./benchmarks/06-mvcc-cost.js');
    await import('./benchmarks/07-vectorized.js');
    collector.save('./results.json');
    resultsJson = fs.readFileSync('./results.json', 'utf8');
    status = 'done';
    console.log('\n=== BENCHMARKS COMPLETE ===');
    console.log('Visit the root URL to see results or /results.json to download.');
  } catch (err) {
    status = 'error: ' + err.message;
    console.error('Benchmark failed:', err);
  }
}
