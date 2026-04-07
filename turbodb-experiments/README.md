# TurboDB storage experiments

Benchmarks comparing database storage formats at the page level. Measures the actual cost of each translation layer between your application code and raw data on disk.

## What it tests

1. **Raw page I/O** — read/write throughput at 512B to 64KB page sizes
2. **Row format overhead** — PostgreSQL-style heap (28B/row overhead) vs compact (2B) vs columnar (8B)
3. **SQLite comparison** — same workloads against a real database engine to measure the cost of SQL parsing, query planning, B-tree traversal, and type serialization

## Run locally

```bash
npm install
npm run bench          # all experiments (~2-3 min)
npm run bench:pages    # just raw I/O
npm run bench:formats  # just format comparison
npm run bench:sqlite   # just the SQLite comparison
```

Results are saved to `results.json` — paste it back to Claude for charts and analysis.

## Deploy to Railway

```bash
# Push to a git repo first, then:
# 1. New project → Deploy from GitHub repo
# 2. Railway will auto-detect the Dockerfile
# 3. After build, check the deploy logs for results
# 4. Or: railway run node src/run-all.js
```

Or use the Railway CLI:
```bash
npm install -g @railway/cli
railway login
railway init
railway up
# Check logs for output + results.json
```

## Deploy to Digital Ocean

**App Platform (easiest):**
```bash
# Push to GitHub, then:
# 1. Create App → pick your repo
# 2. Set as "Worker" (not web service — no HTTP needed)
# 3. Check build logs for results
```

**Droplet (best for real hardware numbers):**
```bash
# Create a droplet (recommend: CPU-optimized, NVMe storage)
# $0.03/hr for a c-4-8gib

ssh root@your-droplet
apt update && apt install -y nodejs npm
git clone <your-repo>
cd turbodb-experiments
npm install
npm run bench
cat results.json  # copy this
# Then destroy the droplet
```

## What results.json contains

```json
{
  "environment": {
    "cpuModel": "...",
    "cpus": 4,
    "totalMemoryGB": 8,
    "disks": "..."
  },
  "experiments": {
    "page_io": { "writes": [...], "reads": [...] },
    "sqlite_compare": {
      "pointLookup": { "sqlitePrepared": { "p50": 14, "opsPerSec": 60271 }, ... },
      "fullScan": { ... },
      "aggregation": { ... },
      "storage": { ... }
    }
  }
}
```

## Safe to run

All operations write to temporary files inside the project directory (cleaned up automatically) or work entirely in memory. No system files, raw devices, or existing data are touched. SQLite runs as WASM in-memory. The whole project can be deleted with no trace.
