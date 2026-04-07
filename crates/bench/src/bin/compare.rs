//! Bench regression gate comparator.
//!
//! Reads the most recent criterion run from `target/criterion/<workload>/new/estimates.json`
//! for each of the seven workloads listed in
//! `docs/superpowers/specs/2026-04-07-bench-regression-gate-design.md`, then
//! compares against two checked-in baseline files:
//!
//! 1. `crates/bench/baseline/main.json`         — per-workload absolute (±7%)
//! 2. `crates/bench/baseline/thesis-ratios.json` — ratio ceilings (e.g. 2.5x)
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

const ABSOLUTE_THRESHOLD: f64 = 0.07; // 7% per-workload tolerance

const WORKLOADS: &[&str] = &[
    "insert_10k",
    "btree_lookup",
    "seq_scan_filter",
    "powql_point",
    "powql_filter_only",
    "powql_filter_projection",
    "powql_aggregation",
];

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
        eprintln!("error: {} workload(s) missing from criterion output", missing.len());
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
        "{:<28} {:>14} {:>14} {:>10} {:>8}",
        "workload", "baseline", "current", "delta", "gate"
    );
    println!("{}", "─".repeat(78));

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

        let (delta_str, gate_str) = match (r.baseline_ns, r.current_ns) {
            (Some(b), Some(c)) => {
                let delta = (c - b) / b;
                let pct = format!("{:+>9.2}%", delta * 100.0);
                if delta > ABSOLUTE_THRESHOLD {
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
            "{:<28} {:>14} {:>14} {:>10} {:>8}",
            r.name, baseline_str, current_str, delta_str, gate_str
        );
    }
    println!();

    // ── Ratio gate ─────────────────────────────────────────────────────────
    let ratio_json = read_json(&ratios_path).unwrap_or_else(|e| {
        eprintln!("warning: could not read thesis-ratios.json ({e}); skipping ratio gate");
        Json::Null
    });

    let ratio_checks = parse_ratios(&ratio_json, &current);

    let mut ratio_failed = false;
    if !ratio_checks.is_empty() {
        println!(
            "{:<32} {:>10} {:>12} {:>8}",
            "ratio", "ceiling", "current", "gate"
        );
        println!("{}", "─".repeat(70));
        for check in &ratio_checks {
            let observed_str = check
                .observed
                .map(|v| format!("{:>10.3}x", v))
                .unwrap_or_else(|| "          —".to_string());
            let gate_str = match check.observed {
                Some(v) if v > check.ceiling => {
                    ratio_failed = true;
                    "FAIL"
                }
                Some(_) => "PASS",
                None => "—",
            };
            println!(
                "{:<32} {:>9.3}x {:>12} {:>8}",
                check.name, check.ceiling, observed_str, gate_str
            );
            println!(
                "  ({} / {})",
                check.numerator, check.denominator
            );
        }
        println!();
    }

    // ── Verdict ────────────────────────────────────────────────────────────
    if absolute_failed || ratio_failed {
        eprintln!("REGRESSION: gate failed.");
        if absolute_failed {
            eprintln!("  - one or more workloads exceeded {:.0}% absolute threshold", ABSOLUTE_THRESHOLD * 100.0);
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
        .ok_or_else(|| format!("missing median.point_estimate in {}", estimates_path.display()))
}

fn parse_ratios(
    ratio_json: &Json,
    current: &BTreeMap<&'static str, f64>,
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

            Some(RatioCheck {
                name: name.clone(),
                numerator,
                denominator,
                ceiling,
                observed,
            })
        })
        .collect()
}
