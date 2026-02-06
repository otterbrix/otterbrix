#!/bin/bash
# Run otterbrix tests with timeout per test case
# Usage: ./run_tests.sh [timeout_seconds] [test_filter]
# Example: ./run_tests.sh 60 "test_collection"
# Example: ./run_tests.sh 120  # run all tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="${PROJECT_ROOT}/build/Release"
TEST_BIN="${BUILD_DIR}/integration/cpp/test/test_otterbrix"

# Default timeout: 60 seconds per test
TIMEOUT=${1:-60}
FILTER=${2:-""}

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if test binary exists
if [ ! -f "$TEST_BIN" ]; then
    echo -e "${RED}Error: Test binary not found at $TEST_BIN${NC}"
    echo "Build the project first: cmake --build build/Release --target test_otterbrix"
    exit 1
fi

cd "$BUILD_DIR/integration/cpp/test"

# Get list of test cases
if [ -n "$FILTER" ]; then
    TEST_CASES=$(./test_otterbrix --list-tests 2>/dev/null | grep -E "^\s+" | sed 's/^[[:space:]]*//' | grep "$FILTER" || true)
else
    TEST_CASES=$(./test_otterbrix --list-tests 2>/dev/null | grep -E "^\s+" | sed 's/^[[:space:]]*//')
fi

if [ -z "$TEST_CASES" ]; then
    echo -e "${YELLOW}No test cases found matching filter: $FILTER${NC}"
    exit 0
fi

TOTAL=0
PASSED=0
FAILED=0
TIMEOUT_COUNT=0

echo "=========================================="
echo "Running tests with ${TIMEOUT}s timeout each"
echo "=========================================="
echo ""

# Run each test case with timeout
while IFS= read -r test_name; do
    [ -z "$test_name" ] && continue
    TOTAL=$((TOTAL + 1))

    echo -n "[$TOTAL] $test_name ... "

    # Create temp file for output
    TMPFILE=$(mktemp)

    # Run with timeout (use perl for cross-platform timeout)
    START_TIME=$(date +%s)

    # macOS doesn't have timeout, so we use a subshell with background process
    (
        ./test_otterbrix "$test_name" > "$TMPFILE" 2>&1 &
        TEST_PID=$!

        # Wait with timeout
        COUNTER=0
        while [ $COUNTER -lt $TIMEOUT ]; do
            if ! kill -0 $TEST_PID 2>/dev/null; then
                break
            fi
            sleep 1
            COUNTER=$((COUNTER + 1))
        done

        # Kill if still running
        if kill -0 $TEST_PID 2>/dev/null; then
            kill -9 $TEST_PID 2>/dev/null
            echo "TIMEOUT" > "${TMPFILE}.status"
        fi

        wait $TEST_PID 2>/dev/null
        echo $? > "${TMPFILE}.exitcode"
    )

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    # Check result
    if [ -f "${TMPFILE}.status" ] && [ "$(cat ${TMPFILE}.status)" = "TIMEOUT" ]; then
        echo -e "${YELLOW}TIMEOUT${NC} (${DURATION}s)"
        TIMEOUT_COUNT=$((TIMEOUT_COUNT + 1))
        FAILED=$((FAILED + 1))
    elif grep -q "All tests passed" "$TMPFILE"; then
        ASSERTIONS=$(grep -oE "[0-9]+ assertions" "$TMPFILE" | head -1 || echo "? assertions")
        echo -e "${GREEN}PASSED${NC} (${DURATION}s, ${ASSERTIONS})"
        PASSED=$((PASSED + 1))
    elif grep -q "FAILED" "$TMPFILE"; then
        echo -e "${RED}FAILED${NC} (${DURATION}s)"
        FAILED=$((FAILED + 1))
        # Show failure details
        echo "  Details:"
        grep -A2 "FAILED" "$TMPFILE" | head -10 | sed 's/^/    /'
    else
        echo -e "${RED}ERROR${NC} (${DURATION}s)"
        FAILED=$((FAILED + 1))
        tail -5 "$TMPFILE" | sed 's/^/    /'
    fi

    # Cleanup
    rm -f "$TMPFILE" "${TMPFILE}.status" "${TMPFILE}.exitcode"

done <<< "$TEST_CASES"

echo ""
echo "=========================================="
echo "Summary:"
echo "  Total:   $TOTAL"
echo -e "  ${GREEN}Passed:  $PASSED${NC}"
echo -e "  ${RED}Failed:  $FAILED${NC}"
if [ $TIMEOUT_COUNT -gt 0 ]; then
    echo -e "  ${YELLOW}Timeout: $TIMEOUT_COUNT${NC}"
fi
echo "=========================================="

# Exit with error if any tests failed
[ $FAILED -eq 0 ]
