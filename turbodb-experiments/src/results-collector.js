/**
 * Results collector — gathers all benchmark data into a single JSON object
 * that can be pasted back for analysis and charting.
 */
import fs from 'node:fs';
import os from 'node:os';
import { execSync } from 'node:child_process';

class ResultsCollector {
  constructor() {
    this.results = {
      environment: this._getEnvironment(),
      experiments: {},
      timestamp: new Date().toISOString(),
    };
  }

  _getEnvironment() {
    const env = {
      platform: os.platform(),
      arch: os.arch(),
      cpus: os.cpus().length,
      cpuModel: os.cpus()[0]?.model || 'unknown',
      totalMemoryGB: Math.round(os.totalmem() / (1024 ** 3) * 10) / 10,
      nodeVersion: process.version,
      kernel: os.release(),
    };

    // Try to detect disk type on Linux
    try {
      const lsblk = execSync('lsblk -d -o NAME,ROTA,TYPE,SIZE,MODEL 2>/dev/null', { encoding: 'utf8' });
      env.disks = lsblk.trim();
    } catch { env.disks = 'unable to detect'; }

    // Try to detect disk type on macOS
    try {
      if (os.platform() === 'darwin') {
        const diskutil = execSync('diskutil info disk0 2>/dev/null | grep -E "Protocol|Medium Type|Solid State|Device"', { encoding: 'utf8' });
        env.disks = diskutil.trim();
      }
    } catch { /* already set above */ }

    return env;
  }

  add(experimentName, data) {
    this.results.experiments[experimentName] = data;
  }

  save(filepath = './results.json') {
    const json = JSON.stringify(this.results, null, 2);
    fs.writeFileSync(filepath, json);
    console.log(`\n  Results saved to ${filepath}`);
    console.log(`  File size: ${(Buffer.byteLength(json) / 1024).toFixed(1)} KB`);
    console.log(`\n  ┌──────────────────────────────────────────────────────────────┐`);
    console.log(`  │  Paste the contents of results.json back to Claude          │`);
    console.log(`  │  for analysis, charts, and comparison against other runs.   │`);
    console.log(`  └──────────────────────────────────────────────────────────────┘\n`);
    return json;
  }

  print() {
    console.log('\n  Environment:');
    const e = this.results.environment;
    console.log(`    ${e.cpuModel} (${e.cpus} cores)`);
    console.log(`    ${e.totalMemoryGB} GB RAM | ${e.platform} ${e.arch} | Node ${e.nodeVersion}`);
    if (e.disks && e.disks !== 'unable to detect') {
      console.log(`    Disk: ${e.disks.split('\n').map(l => l.trim()).join(' | ')}`);
    }
    console.log();
  }
}

// Singleton
const collector = new ResultsCollector();
export default collector;
