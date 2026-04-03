#!/bin/bash

# Quick test script using local test data for DuckDB JSONBench
# Usage: ./test_local.sh [num_records] [--no-cleanup]

NUM_RECORDS="20000"
NO_CLEANUP=false
for arg in "$@"; do
    case "$arg" in
        --no-cleanup) NO_CLEANUP=true ;;
        [0-9]*)       NUM_RECORDS="$arg" ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OTTERBRIX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_FILE="$OTTERBRIX_ROOT/JSONBench/file_0001.json"
DB_FILE="/tmp/bluesky_test.duckdb"
TABLE_NAME="bluesky"

echo "=== DuckDB JSONBench Local Test ==="
echo "Test file: $TEST_FILE"
echo "DB file:   $DB_FILE"
echo "Records:   $NUM_RECORDS"
echo ""

if [[ ! -f "$TEST_FILE" ]]; then
    echo "Error: Test file not found: $TEST_FILE"
    exit 1
fi

# Remove existing DB
rm -f "$DB_FILE" "${DB_FILE}.wal"

# Prepare temp file with limited records
TEMP_FILE=$(mktemp /tmp/bluesky_local.XXXXXX.json)
head -n "$NUM_RECORDS" "$TEST_FILE" > "$TEMP_FILE"

# Create table and load data
echo "=== Creating table and loading data ==="
duckdb "$DB_FILE" <<EOF
CREATE TABLE $TABLE_NAME (j JSON);
INSERT INTO $TABLE_NAME SELECT * FROM read_ndjson_objects('$TEMP_FILE', ignore_errors=true, maximum_object_size=1048576000);
EOF

rm -f "$TEMP_FILE"

# Count records
echo ""
echo "=== Record count ==="
duckdb "$DB_FILE" -c "SELECT count() FROM $TABLE_NAME;"

# Run benchmark queries
echo ""
echo "=== Running Benchmark Queries ==="

TRIES=3

run_query() {
    local query_num=$1
    local query="$2"
    echo ""
    echo "--- Query $query_num ---"
    echo "$query" | head -c 120
    echo "..."

    for i in $(seq 1 $TRIES); do
        duckdb "$DB_FILE" <<EOF
.timer on
$query
EOF
    done
}

# Q1: Top event types
run_query 1 "SELECT j->>'$.commit.collection' AS event, count() AS count FROM $TABLE_NAME GROUP BY event ORDER BY count DESC;"

# Q2: Event types with unique users
run_query 2 "SELECT j->>'$.commit.collection' AS event, count() AS count, count(DISTINCT j->>'$.did') AS users FROM $TABLE_NAME WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create') GROUP BY event ORDER BY count DESC;"

# Q3: Event counts by hour
run_query 3 "SELECT j->>'$.commit.collection' AS event, hour(TO_TIMESTAMP(CAST(j->>'$.time_us' AS BIGINT) / 1000000)) as hour_of_day, count() AS count FROM $TABLE_NAME WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create') AND (j->>'$.commit.collection' in ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like']) GROUP BY event, hour_of_day ORDER BY hour_of_day, event;"

# Q4: First 3 users to post
run_query 4 "SELECT j->>'$.did'::String as user_id, TO_TIMESTAMP(CAST(MIN(j->>'$.time_us') AS BIGINT) / 1000000) AS first_post_date FROM $TABLE_NAME WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create') AND (j->>'$.commit.collection' = 'app.bsky.feed.post') GROUP BY user_id ORDER BY first_post_date ASC LIMIT 3;"

# Q5: Top users by activity span
run_query 5 "SELECT j->>'$.did'::String as user_id, date_diff('milliseconds', TO_TIMESTAMP(CAST(MIN(j->>'$.time_us') AS BIGINT) / 1000000), TO_TIMESTAMP(CAST(MAX(j->>'$.time_us') AS BIGINT) / 1000000)) AS activity_span FROM $TABLE_NAME WHERE (j->>'$.kind' = 'commit') AND (j->>'$.commit.operation' = 'create') AND (j->>'$.commit.collection' = 'app.bsky.feed.post') GROUP BY user_id ORDER BY activity_span DESC LIMIT 3;"

echo ""
if [[ "$NO_CLEANUP" == "true" ]]; then
    echo "=== Skipping cleanup (--no-cleanup) ==="
    echo "DB file kept: $DB_FILE"
    echo "To drop manually: rm -f $DB_FILE ${DB_FILE}.wal"
else
    echo "=== Cleanup ==="
    rm -f "$DB_FILE" "${DB_FILE}.wal"
fi
echo "Done."
