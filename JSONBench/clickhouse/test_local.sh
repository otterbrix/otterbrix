#!/bin/bash

# Quick test script using local test data for ClickHouse JSONBench
# Usage: ./test_local.sh [num_records] [--no-cleanup]

NUM_RECORDS="${1:-20000}"
NO_CLEANUP=false
if [[ "$1" == "--no-cleanup" ]]; then
    NO_CLEANUP=true
    NUM_RECORDS="20000"
elif [[ "$2" == "--no-cleanup" ]]; then
    NO_CLEANUP=true
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OTTERBRIX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_FILE="$OTTERBRIX_ROOT/JSONBench/file_0001.json"
DB_NAME="bluesky_test"
TABLE_NAME="bluesky"
CH="clickhouse-client --password 1234"

echo "=== ClickHouse JSONBench Local Test ==="
echo "Test file: $TEST_FILE"
echo "Database: $DB_NAME"
echo "Records: $NUM_RECORDS"
echo ""

# Check if test file exists
if [[ ! -f "$TEST_FILE" ]]; then
    echo "Error: Test file not found: $TEST_FILE"
    exit 1
fi

# Drop existing database if exists
echo "=== Dropping existing database ==="
$CH --query "DROP DATABASE IF EXISTS $DB_NAME"

# Create database
echo "=== Creating database ==="
$CH --query "CREATE DATABASE $DB_NAME"

# Create table
echo "=== Creating table ==="
$CH --database="$DB_NAME" --enable_json_type=1 --multiquery < "$SCRIPT_DIR/ddl.sql"

# Load data
echo "=== Loading data (first $NUM_RECORDS records) ==="
head -n "$NUM_RECORDS" "$TEST_FILE" | \
    $CH --database="$DB_NAME" \
        --query="INSERT INTO $TABLE_NAME SETTINGS min_insert_block_size_rows=1000000, min_insert_block_size_bytes=0 FORMAT JSONAsObject"

# Count records
echo ""
echo "=== Record count ==="
$CH --database="$DB_NAME" --query="SELECT count() FROM $TABLE_NAME"

# Run benchmark queries
echo ""
echo "=== Running Benchmark Queries ==="

TRIES=3

run_query() {
    local query_num=$1
    local query="$2"
    echo ""
    echo "--- Query $query_num ---"
    echo "$query" | head -c 100
    echo "..."

    for i in $(seq 1 $TRIES); do
        $CH --database="$DB_NAME" --time --format=Null --query="$query" 2>&1 | grep -v "^$"
    done
}

# Q1: Top event types
run_query 1 "SELECT data.commit.collection AS event, count() AS count FROM $TABLE_NAME GROUP BY event ORDER BY count DESC;"

# Q2: Event types with unique users
run_query 2 "SELECT data.commit.collection AS event, count() AS count, uniqExact(data.did) AS users FROM $TABLE_NAME WHERE data.kind = 'commit' AND data.commit.operation = 'create' GROUP BY event ORDER BY count DESC;"

# Q3: Event counts by hour
run_query 3 "SELECT data.commit.collection AS event, toHour(fromUnixTimestamp64Micro(data.time_us)) as hour_of_day, count() AS count FROM $TABLE_NAME WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection in ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like'] GROUP BY event, hour_of_day ORDER BY hour_of_day, event;"

# Q4: First 3 users to post
run_query 4 "SELECT data.did::String as user_id, min(fromUnixTimestamp64Micro(data.time_us)) as first_post_ts FROM $TABLE_NAME WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_ts ASC LIMIT 3;"

# Q5: Top users by activity span
run_query 5 "SELECT data.did::String as user_id, date_diff('milliseconds', min(fromUnixTimestamp64Micro(data.time_us)), max(fromUnixTimestamp64Micro(data.time_us))) AS activity_span FROM $TABLE_NAME WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY activity_span DESC LIMIT 3;"

echo ""
if [[ "$NO_CLEANUP" == "true" ]]; then
    echo "=== Skipping cleanup (--no-cleanup) ==="
    echo "Database '$DB_NAME' kept. To drop manually:"
    echo "  clickhouse-client --password 1234 --query \"DROP DATABASE $DB_NAME\""
else
    echo "=== Cleanup ==="
    $CH --query "DROP DATABASE $DB_NAME"
fi
echo "Done."
