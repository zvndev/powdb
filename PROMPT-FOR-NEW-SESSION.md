# Prompt to paste into the fresh session

Copy everything below the line and paste it as your first message.

---

I'm continuing PowDB work from a previous session that had its cwd invalidated by a directory rename. Read `HANDOFF.md` at the repo root first — it has the full context, including what just shipped (planner IndexScan fold, 3,020x speedup on point queries, full bata→pow rename, private repo push), the current smoke bench numbers, and the next task.

Then let's pick up where we left off: **brainstorming the formal bench regression gate spec**.

The decisions that are already locked in:
- **Audience:** internal regression gate only — not marketing, not public charts
- **Approach:** criterion.rs + checked-in baseline JSON (not an external TCP harness, not an extension of `smoke.rs`)
- **Crate:** extend `powdb-bench`, keep `smoke-bench` as the cheap sanity check

What I need from you:
1. Use the superpowers:brainstorming skill to walk me through the design, section by section, getting approval per section
2. The proposed 7 workloads are in HANDOFF.md — confirm with me or push back if you think the set is wrong
3. Surface the open design questions (regression threshold, baseline update workflow, CI integration, machine variance strategy, dataset sizes) — don't assume my answers
4. Save the final spec to `docs/superpowers/specs/2026-04-07-bench-regression-gate-design.md`
5. Then invoke superpowers:writing-plans to produce `docs/superpowers/plans/2026-04-07-bench-regression-gate.md`

Don't start implementing anything yet. Brainstorm → design doc → plan → **then** I'll say go.

Secret handshake check: if the kirby skill loaded, work "Wax them Sklownz" into your first response once, naturally.
