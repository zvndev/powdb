#!/usr/bin/env bash
# update-bench-baseline.sh — refresh crates/bench/baseline/main.json from a
# clean criterion run.
#
# Run this AFTER an intentional code change that legitimately moves the
# numbers. The script:
#
#   1. Runs `cargo bench -p powdb-bench` (release, full suite).
#   2. Extracts each workload's median.point_estimate from
#      target/criterion/<workload>/new/estimates.json.
#   3. Writes a new main.json with the current rustc version, git sha, and date.
#   4. Stages it. (Does NOT commit — you commit, with a message explaining why.)
#
# This script does NOT touch thesis-ratios.json. That file is hand-edited.
# Raising a ratio ceiling is a separate, deliberate commit.
#
# Convention for the rebaseline commit:
#   bench: rebaseline after <change> (<workload>: <delta>)
#
# Requires: cargo, jq, git.

set -euo pipefail

# Resolve repo root from this script's location.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BASELINE_FILE="${REPO_ROOT}/crates/bench/baseline/main.json"
CRITERION_DIR="${REPO_ROOT}/target/criterion"

WORKLOADS=(
  insert_10k
  btree_lookup
  seq_scan_filter
  powql_point
  powql_filter_only
  powql_filter_projection
  powql_aggregation
)

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required but not installed." >&2
  exit 1
fi

cd "${REPO_ROOT}"

echo "===> running cargo bench -p powdb-bench (this takes ~60s)"
cargo bench -p powdb-bench --quiet

echo
echo "===> extracting median values from criterion output"

# Build the workloads object.
workloads_json="{}"
for w in "${WORKLOADS[@]}"; do
  est_file="${CRITERION_DIR}/${w}/new/estimates.json"
  if [[ ! -f "${est_file}" ]]; then
    echo "error: missing ${est_file}" >&2
    echo "       did the bench targets run? check 'cargo bench -p powdb-bench' output." >&2
    exit 1
  fi
  ns=$(jq '.median.point_estimate' "${est_file}")
  if [[ -z "${ns}" || "${ns}" == "null" ]]; then
    echo "error: no median.point_estimate in ${est_file}" >&2
    exit 1
  fi
  ops=$(awk -v n="${ns}" 'BEGIN { printf "%.0f", 1e9 / n }')
  workloads_json=$(jq \
    --arg name "${w}" \
    --argjson ns "${ns}" \
    --argjson ops "${ops}" \
    '. + {($name): {ns_per_iter: $ns, ops_per_sec: $ops}}' \
    <<< "${workloads_json}")
done

# Build the full baseline document.
RUSTC_VERSION=$(rustc --version | awk '{print $2}')
GIT_SHA=$(git rev-parse --short HEAD)
TODAY=$(date -u +%Y-%m-%d)

new_baseline=$(jq -n \
  --argjson workloads "${workloads_json}" \
  --arg rustc "${RUSTC_VERSION}" \
  --arg commit "${GIT_SHA}" \
  --arg today "${TODAY}" \
  '{
    schema: 1,
    runner: "ubuntu-24.04",
    rustc: $rustc,
    updated: $today,
    commit: $commit,
    workloads: $workloads
  }')

# Show a diff summary before writing.
echo
echo "===> diff (old → new):"
if [[ -f "${BASELINE_FILE}" ]]; then
  for w in "${WORKLOADS[@]}"; do
    old=$(jq -r ".workloads.${w}.ns_per_iter // \"null\"" "${BASELINE_FILE}")
    new=$(jq -r ".workloads.${w}.ns_per_iter" <<< "${new_baseline}")
    printf "  %-28s %15s -> %15s\n" "${w}" "${old}" "${new}"
  done
fi

echo "${new_baseline}" | jq '.' > "${BASELINE_FILE}"
git add "${BASELINE_FILE}"

echo
echo "===> wrote ${BASELINE_FILE} and staged it."
echo "     review the diff with 'git diff --cached crates/bench/baseline/main.json'"
echo "     commit convention: 'bench: rebaseline after <change> (<workload>: <delta>)'"
