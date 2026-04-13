//! Bench regression gate comparator.
//!
//! Reads the most recent criterion run from `target/criterion/<workload>/new/estimates.json`
//! for every workload in `WORKLOADS`, then compares against two checked-in
//! baseline files:
//!
//! 1. `crates/bench/baseline/main.json`         — per-workload absolute (±7% default, ±10% for noisy)
//! 2. `crates/bench/baseline/thesis-ratios.json` — ratio ceilings (e.g. 2.5x)
//!
//! The workload list covers:
//!   - Storage-layer guards (insert_10k, btree_lookup, seq_scan_filter).
//!   - Legacy PowQL guards (powql_point, powql_filter_only,
//!     powql_filter_projection, powql_aggregation) — workloads 1 and 3 of
//!     PLAN-MISSION-A.md §1 reuse `powql_point` and `powql_aggregation`
//!     respectively for gate continuity.
//!   - Mission A expansion workloads 2, 4-15 from PLAN-MISSION-A.md §1
//!     (point_lookup_nonindexed, scan_filter_project_top100,
//!     scan_filter_sort_limit10, agg_sum/avg/min/max, multi_col_and_filter,
//!     insert_single, insert_batch_1k, update_by_pk, update_by_filter,
//!     delete_by_filter).
//!
//! Exits 0 on pass, 1 on regression. Tolerates `null` baseline values for
//! the absolute gate so the very first run on a fresh runner can capture
//! initial numbers without failing — the comparator prints what it observed
//! so the human can paste it into `main.json` for the real baseline.
//!
//! Usage:
//!
//! ```bash
//! cargo bench -p powdb-bench
//! cargo run -p powdb-bench --bin compare
//! ```

use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Default ±7% per-workload tolerance. Matches the storage-layer and legacy
/// PowQL bench noise floor observed on ubuntu-24.04 and M1.
const DEFAULT_ABSOLUTE_THRESHOLD: f64 = 0.07;

/// Relaxed ±10% tolerance for the Mission A workloads that run a tight
/// per-iteration setup or hit paths with more jitter (insert/update/delete
/// loops, sort+limit over a filtered scan). These workloads have been
/// measured to fluctuate 3-8% across identical local runs on M1 — the extra
/// margin keeps the gate honest without flapping.
const NOISY_ABSOLUTE_THRESHOLD: f64 = 0.10;

/// ±20% tolerance for the sub-millisecond aggregation + point-probe workloads
/// that are dominated by Azure-pool ubuntu-24.04 runner-to-runner variance
/// rather than any structural property of the code. Evidence: two identical
/// `--bench` runs on the same merge commit (07fcaa6, runs 24213258406 and
/// 24213724490, fifteen minutes apart) produced agg_sum 385ms vs 504ms
/// (+31% spread), agg_min 362ms vs 467ms (+29%), powql_aggregation 545ms vs
/// 624ms (+14%) — with NO code change between runs. These workloads all
/// scan ~100K rows of ~5μs work each, so ~100μs of runner scheduling noise
/// is a 20-30% delta. Widening to 20% + pinning the baseline at the high
/// end of observed noise keeps the gate honest against real regressions
/// (which would have to exceed even the slowest runner by another 20%)
/// without flapping on every fresh Azure VM assignment.
const VERY_NOISY_ABSOLUTE_THRESHOLD: f64 = 0.20;

const WORKLOADS: &[&str] = &[
    // ── Storage layer (ratio denominator + existing guards) ──
    "insert_10k",
    "btree_lookup",
    "seq_scan_filter",
    // ── Legacy PowQL guards + Mission A workloads 1 & 3 ──
    "powql_point",             // MA#1 point_lookup_indexed
    "powql_filter_only",       // legacy 5a
    "powql_filter_projection", // legacy 5b
    "powql_aggregation",       // MA#3 scan_filter_count
    // ── Mission A reads (workloads 2, 4-10) ──
    "point_lookup_nonindexed",    // MA#2
    "scan_filter_project_top100", // MA#4
    "scan_filter_sort_limit10",   // MA#5
    "agg_sum",                    // MA#6
    "agg_avg",                    // MA#7
    "agg_min",                    // MA#8
    "agg_max",                    // MA#9
    "multi_col_and_filter",       // MA#10
    // ── Mission A writes (workloads 11-15) ──
    "insert_single",    // MA#11
    "insert_batch_1k",  // MA#12
    "update_by_pk",     // MA#13
    "update_by_filter", // MA#14
    "delete_by_filter", // MA#15
];

/// Return the absolute-threshold that applies to a workload. Most workloads
/// use the ±7% default; a handful of write-heavy or sort-heavy workloads
/// get ±10% because their per-iter work is chunkier and the variance wider.
fn threshold_for(workload: &str) -> f64 {
    match workload {
        // Sub-millisecond aggregation + point-probe workloads where
        // Azure-pool GHA runner variance dominates over any structural
        // perf delta. See VERY_NOISY_ABSOLUTE_THRESHOLD comment above for
        // the evidence chain (back-to-back same-commit runs with +14 to
        // +31% spread across these workloads).
        "agg_sum"
        | "agg_avg"
        | "agg_min"
        | "agg_max"
        | "powql_aggregation"
        | "point_lookup_nonindexed" => VERY_NOISY_ABSOLUTE_THRESHOLD,

        // GHA-variance-dominated workloads: back-to-back same-commit PR #9
        // runs showed scan_filter_sort_limit10 +11.9%, update_by_pk +86%,
        // delete_by_filter +17.7% — all with zero code change. Promoted
        // from NOISY (10%) to VERY_NOISY (20%). update_by_filter promoted
        // in PR #14 after +13.9% variance on identical code.
        "scan_filter_sort_limit10" | "update_by_pk" | "delete_by_filter" | "update_by_filter" => {
            VERY_NOISY_ABSOLUTE_THRESHOLD
        }

        // Bulk writes and multi-column scans: fixture growth, WAL sync,
        // btree splits — naturally more variance than point reads, but not
        // as extreme as the above. multi_col_and_filter promoted in PR #15
        // after +10.14% variance on identical code (four same-code runs
        // showed 2.89%–10.14% spread).
        "insert_single" | "insert_batch_1k" | "multi_col_and_filter" => NOISY_ABSOLUTE_THRESHOLD,
        _ => DEFAULT_ABSOLUTE_THRESHOLD,
    }
}

#[derive(Debug)]
struct WorkloadResult {
    name: String,
    current_ns: Option<f64>,
    baseline_ns: Option<f64>,
}

#[derive(Debug)]
struct RatioCheck {
    name: String,
    numerator: String,
    denominator: String,
    ceiling: f64,
    observed: Option<f64>,
    /// False when either endpoint has a null baseline entry. Used to keep the
    /// CRITERION→FASTPATH race quiet during FIRST-RUN CAPTURE: if the
    /// baseline can't tell us what "good" looks like yet, we print the
    /// current ratio for humans but don't fail the gate.
    enforced: bool,
}

fn main() -> ExitCode {
    let manifest_dir = env_manifest_dir();
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    let criterion_dir = workspace_root.join("target/criterion");
    let baseline_path = manifest_dir.join("baseline/main.json");
    let ratios_path = manifest_dir.join("baseline/thesis-ratios.json");

    println!("PowDB bench regression gate");
    println!("  criterion dir : {}", criterion_dir.display());
    println!("  baseline      : {}", baseline_path.display());
    println!("  ratios        : {}", ratios_path.display());
    println!();

    // ── Load current run estimates ─────────────────────────────────────────
    let mut current: BTreeMap<&'static str, f64> = BTreeMap::new();
    let mut missing: Vec<&'static str> = vec![];
    for &workload in WORKLOADS {
        match read_estimate_median(&criterion_dir, workload) {
            Ok(ns) => {
                current.insert(workload, ns);
            }
            Err(e) => {
                eprintln!("warning: no criterion estimate for {workload}: {e}");
                missing.push(workload);
            }
        }
    }

    if !missing.is_empty() {
        eprintln!();
        eprintln!(
            "error: {} workload(s) missing from criterion output",
            missing.len()
        );
        eprintln!("       did `cargo bench -p powdb-bench` run all benches?");
        return ExitCode::from(1);
    }

    // ── Load baseline (allow nulls for first-run capture) ──────────────────
    let baseline_json = read_json(&baseline_path).unwrap_or_else(|e| {
        eprintln!("warning: could not read baseline ({e}); treating as first-run capture");
        Json::Null
    });

    let baseline_workloads = baseline_json
        .get("workloads")
        .and_then(Json::as_object)
        .cloned()
        .unwrap_or_default();

    let mut results: Vec<WorkloadResult> = Vec::with_capacity(WORKLOADS.len());
    for &workload in WORKLOADS {
        let baseline_ns = baseline_workloads
            .get(workload)
            .and_then(|w| w.get("ns_per_iter"))
            .and_then(Json::as_f64);
        results.push(WorkloadResult {
            name: workload.to_string(),
            current_ns: current.get(workload).copied(),
            baseline_ns,
        });
    }

    // ── Print absolute gate table ──────────────────────────────────────────
    println!(
        "{:<28} {:>14} {:>14} {:>10} {:>6} {:>8}",
        "workload", "baseline", "current", "delta", "thr", "gate"
    );
    println!("{}", "─".repeat(86));

    let mut absolute_failed = false;
    let mut first_run_capture = false;
    for r in &results {
        let baseline_str = r
            .baseline_ns
            .map(|ns| format!("{:>10.0} ns", ns))
            .unwrap_or_else(|| "        null".to_string());
        let current_str = r
            .current_ns
            .map(|ns| format!("{:>10.0} ns", ns))
            .unwrap_or_else(|| "        n/a".to_string());

        let threshold = threshold_for(&r.name);
        let threshold_str = format!("{:>4.0}%", threshold * 100.0);

        let (delta_str, gate_str) = match (r.baseline_ns, r.current_ns) {
            (Some(b), Some(c)) => {
                let delta = (c - b) / b;
                let pct = format!("{:+>9.2}%", delta * 100.0);
                if delta > threshold {
                    absolute_failed = true;
                    (pct, "FAIL".to_string())
                } else {
                    (pct, "PASS".to_string())
                }
            }
            (None, Some(_)) => {
                first_run_capture = true;
                ("       —".to_string(), "CAPTURE".to_string())
            }
            _ => ("       —".to_string(), "—".to_string()),
        };

        println!(
            "{:<28} {:>14} {:>14} {:>10} {:>6} {:>8}",
            r.name, baseline_str, current_str, delta_str, threshold_str, gate_str
        );
    }
    println!();

    // ── Ratio gate ─────────────────────────────────────────────────────────
    let ratio_json = read_json(&ratios_path).unwrap_or_else(|e| {
        eprintln!("warning: could not read thesis-ratios.json ({e}); skipping ratio gate");
        Json::Null
    });

    // Build the baseline-ns map keyed by workload name for the ratio gate's
    // "only enforce when both endpoints have non-null baselines" rule. This
    // keeps the CRITERION→FASTPATH race (§4) quiet: pre-FASTPATH, any ratio
    // whose endpoints are still null in main.json will CAPTURE rather than
    // FAIL, even if the observed ratio exceeds the ceiling. Once FASTPATH
    // lands and the rebaseline commit populates the baseline numbers, the
    // ratio switches to enforcing mode automatically.
    let baseline_ns_map: BTreeMap<String, Option<f64>> = results
        .iter()
        .map(|r| (r.name.clone(), r.baseline_ns))
        .collect();

    let ratio_checks = parse_ratios(&ratio_json, &current, &baseline_ns_map);

    let mut ratio_failed = false;
    if !ratio_checks.is_empty() {
        println!(
            "{:<36} {:>10} {:>12} {:>10}",
            "ratio", "ceiling", "current", "gate"
        );
        println!("{}", "─".repeat(74));
        for check in &ratio_checks {
            let observed_str = check
                .observed
                .map(|v| format!("{:>10.3}x", v))
                .unwrap_or_else(|| "          —".to_string());
            let gate_str = match check.observed {
                Some(v) if v > check.ceiling => {
                    if check.enforced {
                        ratio_failed = true;
                        "FAIL"
                    } else {
                        // Endpoint baselines still null: CAPTURE mode.
                        first_run_capture = true;
                        "CAPTURE"
                    }
                }
                Some(_) => {
                    if check.enforced {
                        "PASS"
                    } else {
                        "CAPTURE"
                    }
                }
                None => "—",
            };
            println!(
                "{:<36} {:>9.3}x {:>12} {:>10}",
                check.name, check.ceiling, observed_str, gate_str
            );
            println!("  ({} / {})", check.numerator, check.denominator);
        }
        println!();
    }

    // ── Verdict ────────────────────────────────────────────────────────────
    if absolute_failed || ratio_failed {
        eprintln!("REGRESSION: gate failed.");
        if absolute_failed {
            eprintln!(
                "  - one or more workloads exceeded their absolute threshold ({:.0}% default, {:.0}% for noisy write/sort workloads)",
                DEFAULT_ABSOLUTE_THRESHOLD * 100.0,
                NOISY_ABSOLUTE_THRESHOLD * 100.0,
            );
        }
        if ratio_failed {
            eprintln!("  - one or more thesis ratios exceeded their ceiling");
        }
        eprintln!();
        eprintln!("If this regression is intentional:");
        eprintln!("  - rerun ./scripts/update-bench-baseline.sh and commit the new main.json");
        eprintln!("  - or hand-edit thesis-ratios.json with a justification commit");
        return ExitCode::from(1);
    }

    if first_run_capture {
        println!("FIRST-RUN CAPTURE: baseline had null values for some workloads.");
        println!("  Paste the current values above into crates/bench/baseline/main.json");
        println!("  to set the real baseline, then commit.");
    } else {
        println!("OK: all workloads within threshold, all ratios within ceiling.");
    }
    ExitCode::SUCCESS
}

fn env_manifest_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR is set when run via cargo. When run as a standalone
    // binary, fall back to the current dir.
    std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}

fn read_json(path: &Path) -> Result<Json, String> {
    let content = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_str(&content).map_err(|e| format!("{}: {e}", path.display()))
}

fn read_estimate_median(criterion_dir: &Path, workload: &str) -> Result<f64, String> {
    let estimates_path = criterion_dir
        .join(workload)
        .join("new")
        .join("estimates.json");
    let json = read_json(&estimates_path)?;
    json.get("median")
        .and_then(|m| m.get("point_estimate"))
        .and_then(Json::as_f64)
        .ok_or_else(|| {
            format!(
                "missing median.point_estimate in {}",
                estimates_path.display()
            )
        })
}

fn parse_ratios(
    ratio_json: &Json,
    current: &BTreeMap<&'static str, f64>,
    baseline_ns_map: &BTreeMap<String, Option<f64>>,
) -> Vec<RatioCheck> {
    let Some(ratios) = ratio_json.get("ratios").and_then(Json::as_object) else {
        return vec![];
    };
    ratios
        .iter()
        .filter_map(|(name, def)| {
            let numerator = def.get("numerator")?.as_str()?.to_string();
            let denominator = def.get("denominator")?.as_str()?.to_string();
            let ceiling = def.get("ceiling")?.as_f64()?;

            let observed = match (
                current.get(numerator.as_str()),
                current.get(denominator.as_str()),
            ) {
                (Some(&n), Some(&d)) if d > 0.0 => Some(n / d),
                _ => None,
            };

            // Enforce only when BOTH endpoints have non-null baselines. This
            // implements the CRITERION→FASTPATH race resolution from
            // PLAN-MISSION-A.md §4: pre-FASTPATH, ratios with null endpoints
            // CAPTURE rather than FAIL, and the rebaseline commit flips them
            // to enforcing mode by populating the baseline values.
            let num_baseline = baseline_ns_map.get(&numerator).copied().unwrap_or(None);
            let den_baseline = baseline_ns_map.get(&denominator).copied().unwrap_or(None);
            let enforced = num_baseline.is_some() && den_baseline.is_some();

            Some(RatioCheck {
                name: name.clone(),
                numerator,
                denominator,
                ceiling,
                observed,
                enforced,
            })
        })
        .collect()
}
