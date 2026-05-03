#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VALIDATOR="${VALIDATOR:-$ROOT_DIR/.venv-substrait-validator/bin/substrait-validator}"
FIXTURE_DIR="${FIXTURE_DIR:-$ROOT_DIR/components/planner/test/interop_fixtures}"

STANDARD_STRICT_FIXTURES=(
  "read_named_table.json"
  "filter_true.json"
  "project_single_field.json"
  "sort_single_field.json"
  "fetch_limit_10.json"
  "join_left.json"
)

AGGREGATE_FIXTURE="aggregate_count.json"

VENDOR_FIXTURES=(
  "function_extension_single.json"
  "ddl_create_index.json"
)

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

if [[ ! -x "$VALIDATOR" ]]; then
  echo "Validator not found or not executable: $VALIDATOR" >&2
  echo "Expected local install in: $ROOT_DIR/.venv-substrait-validator" >&2
  exit 2
fi

if [[ ! -d "$FIXTURE_DIR" ]]; then
  echo "Fixture directory not found: $FIXTURE_DIR" >&2
  exit 2
fi

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

print_section "Validator"
"$VALIDATOR" --version
"$VALIDATOR" --substrait-version

print_section "Standard Fixtures (strict)"
for name in "${STANDARD_STRICT_FIXTURES[@]}"; do
  run_expect_success \
    "$name strict" \
    "$VALIDATOR" "$FIXTURE_DIR/$name" --in-type json --out-type diag -m strict
done

print_section "Aggregate Fixture"
run_expect_success \
  "$AGGREGATE_FIXTURE loose" \
  "$VALIDATOR" "$FIXTURE_DIR/$AGGREGATE_FIXTURE" --in-type json --out-type diag -m loose --uri-depth 0

run_expect_failure \
  "$AGGREGATE_FIXTURE strict" \
  "$VALIDATOR" "$FIXTURE_DIR/$AGGREGATE_FIXTURE" --in-type json --out-type diag -m strict --uri-depth 0

print_section "Vendor Fixtures"
for name in "${VENDOR_FIXTURES[@]}"; do
  run_expect_failure \
    "$name strict" \
    "$VALIDATOR" "$FIXTURE_DIR/$name" \
      --in-type json \
      --out-type diag \
      -m strict \
      --allow-proto-any type.googleapis.com/google.protobuf.Struct
done

print_section "Summary"
if [[ "$overall_status" -eq 0 ]]; then
  echo -e "${GREEN}All fixture checks matched expected outcomes.${NC}"
else
  echo -e "${RED}Some fixture checks did not match expected outcomes.${NC}"
fi

exit "$overall_status"
