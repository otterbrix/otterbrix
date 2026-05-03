#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
EXPORT_DIR="${EXPORT_DIR:-$BUILD_DIR/substrait_exports}"
VALIDATOR="${VALIDATOR:-$ROOT_DIR/.venv-substrait-validator/bin/substrait-validator}"
EXPORTER_BIN="${EXPORTER_BIN:-$BUILD_DIR/components/planner/test/substrait_export_samples}"

STANDARD_EXPORTS=(
  "read_named_table.json"
  "filter_true.json"
  "project_single_field.json"
  "sort_single_field.json"
  "fetch_limit_10.json"
  "join_left.json"
)

AGGREGATE_EXPORT="aggregate_count.json"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_section() {
  echo
  echo -e "${YELLOW}>>> $1${NC}"
}

pass() {
  echo -e "${GREEN}PASS${NC}  $1"
}

fail() {
  echo -e "${RED}FAIL${NC}  $1"
}

overall_status=0

run_expect_success() {
  local label="$1"
  shift
  if "$@"; then
    pass "$label"
  else
    fail "$label"
    overall_status=1
  fi
}

run_expect_failure() {
  local label="$1"
  shift
  if "$@"; then
    fail "$label (unexpected success)"
    overall_status=1
  else
    pass "$label (failed as expected)"
  fi
}

if [[ ! -x "$VALIDATOR" ]]; then
  echo "Validator not found or not executable: $VALIDATOR" >&2
  exit 2
fi

print_section "Build Exporter"
cmake --build "$BUILD_DIR" --target substrait_export_samples -j1

if [[ ! -x "$EXPORTER_BIN" ]]; then
  echo "Exporter binary not found: $EXPORTER_BIN" >&2
  exit 2
fi

print_section "Generate Exports"
rm -rf "$EXPORT_DIR"
mkdir -p "$EXPORT_DIR"
"$EXPORTER_BIN" "$EXPORT_DIR"

print_section "Validator"
"$VALIDATOR" --version
"$VALIDATOR" --substrait-version

print_section "Generated Standard Exports (loose)"
for name in "${STANDARD_EXPORTS[@]}"; do
  run_expect_success \
    "$name loose" \
    "$VALIDATOR" "$EXPORT_DIR/$name" --in-type json --out-type diag -m loose
done

print_section "Generated Aggregate Export"
run_expect_success \
  "$AGGREGATE_EXPORT loose" \
  "$VALIDATOR" "$EXPORT_DIR/$AGGREGATE_EXPORT" --in-type json --out-type diag -m loose --uri-depth 0

run_expect_failure \
  "$AGGREGATE_EXPORT strict" \
  "$VALIDATOR" "$EXPORT_DIR/$AGGREGATE_EXPORT" --in-type json --out-type diag -m strict --uri-depth 0

print_section "Summary"
if [[ "$overall_status" -eq 0 ]]; then
  echo -e "${GREEN}All generated export checks matched expected outcomes.${NC}"
else
  echo -e "${RED}Some generated export checks did not match expected outcomes.${NC}"
fi

exit "$overall_status"
