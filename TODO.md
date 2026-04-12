# PowDB — Production Roadmap
Updated: 2026-04-12

## Active Sprints

### Sprint A — Docs & Getting Started *(ACTIVE)*
Target: 1–2 days

- [ ] A-01: Getting Started guide (install, run, first queries)
- [ ] A-02: Deployment guide (Docker, Fly.io, bare metal)
- [ ] A-03: CONTRIBUTING.md + CHANGELOG.md
- [ ] A-04: Publish TS client to npm (`@zvndev/powdb-client`)
- [ ] A-05: Update README benchmark claims — qualify "3–9×" to aggregate/sort workloads

### Sprint B — Core Missing Features *(DONE)*
Target: 3–5 days

- [x] B-01: Graceful shutdown (signal handler + in-flight drain)
- [x] B-02: Connection timeouts (idle + query)
- [x] B-03: CAST / type conversion expressions
- [x] B-04: Math functions (ABS, ROUND, CEIL, FLOOR, SQRT, POW)
- [x] B-05: Date/time functions (NOW, EXTRACT, DATE_ADD, DATEDIFF)
- [x] B-06: UPSERT implementation (token exists, executor missing)
- [x] B-07: Range scan via B+tree index (col > X, col BETWEEN X AND Y)

### Sprint C — Security & Ops *(ACTIVE)*
Target: 2–3 days

- [ ] C-01: TLS support (rustls)
- [ ] C-02: Prometheus metrics endpoint
- [ ] C-03: Slow query logging (configurable threshold)
- [ ] C-04: Query timeout / cancellation
- [ ] C-05: Release automation (GitHub Actions — tag → build → GitHub Release)
- [ ] C-06: Add fuzz targets to CI on schedule (nightly cron)

## Completed

### Hardening Sprint (2026-04-10 → 2026-04-12) — 21/21 ✓
See [SPRINT-PLAN.md](SPRINT-PLAN.md) for full details.
- All CRITICAL + HIGH security vulnerabilities fixed
- EXPLAIN, hash joins, window functions, correlated subqueries
- CI (clippy + fmt + test), MIT license, proptest, fuzz targets
