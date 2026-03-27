#!/bin/bash

# Quick test script using local test data for PostgreSQL JSONBench
# Usage: ./test_local.sh <index|noindex> [num_records] [--single-thread] [--no-cleanup]

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <index|noindex> [num_records] [--single-thread] [--no-cleanup]"
    echo "  index           - create index on JSONB fields"
    echo "  noindex         - no index (full table scan)"
    echo "  --single-thread - disable parallel query execution"
    echo "  --no-cleanup    - keep database after test"
    exit 1
fi

USE_INDEX="$1"
if [[ "$USE_INDEX" != "index" && "$USE_INDEX" != "noindex" ]]; then
    echo "Error: First argument must be 'index' or 'noindex'"
    echo "Usage: $0 <index|noindex> [num_records] [--single-thread] [--no-cleanup]"
    exit 1
fi

NUM_RECORDS="100000"
SINGLE_THREAD=false
NO_CLEANUP=false

for arg in "${@:2}"; do
    case "$arg" in
        --single-thread) SINGLE_THREAD=true ;;
        --no-cleanup)    NO_CLEANUP=true ;;
        [0-9]*)          NUM_RECORDS="$arg" ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OTTERBRIX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_FILE="$OTTERBRIX_ROOT/JSONBench/file_0001.json"
DB_NAME="bluesky_test"
TABLE_NAME="bluesky"

echo "=== PostgreSQL JSONBench Local Test ==="
echo "Test file: $TEST_FILE"
echo "Database: $DB_NAME"
echo "Records: $NUM_RECORDS"
echo "Index: $USE_INDEX"
echo "Single thread: $SINGLE_THREAD"
echo ""

# Check if test file exists
if [[ ! -f "$TEST_FILE" ]]; then
    echo "Error: Test file not found: $TEST_FILE"
    exit 1
fi

# Drop existing database if exists
echo "=== Dropping existing database ==="
sudo -u postgres psql -c "DROP DATABASE IF EXISTS $DB_NAME;" 2>/dev/null

# Create database
echo "=== Creating database ==="
sudo -u postgres psql -c "CREATE DATABASE $DB_NAME;"

# Create table
echo "=== Creating table ==="
sudo -u postgres psql -d $DB_NAME -c "
CREATE TABLE $TABLE_NAME (
    data JSONB COMPRESSION lz4 NOT NULL
);
"

# Create index if requested
if [[ "$USE_INDEX" == "index" ]]; then
    echo "=== Creating index ==="
    sudo -u postgres psql -d $DB_NAME -c "
    CREATE INDEX idx_bluesky
    ON $TABLE_NAME (
        (data ->> 'kind'),
        (data -> 'commit' ->> 'operation'),
        (data -> 'commit' ->> 'collection'),
        (data ->> 'did'),
        (TO_TIMESTAMP((data ->> 'time_us')::BIGINT / 1000000.0))
    );
    "
else
    echo "=== Skipping index creation ==="
fi

# Load data - need to preprocess the file (remove null chars) and use COPY
echo "=== Loading data (${NUM_RECORDS} records) ==="
TEMP_FILE=$(mktemp)
# Remove null characters, limit to NUM_RECORDS rows, and prepare for PostgreSQL COPY
sed 's/\\u0000//g' "$TEST_FILE" | head -n "$NUM_RECORDS" > "$TEMP_FILE"
chmod 644 "$TEMP_FILE"

sudo -u postgres psql -d $DB_NAME -c "\COPY $TABLE_NAME FROM '$TEMP_FILE' WITH (format csv, quote e'\x01', delimiter e'\x02', escape e'\x01');"
rm -f "$TEMP_FILE"

# Count records
echo ""
echo "=== Record count ==="
sudo -u postgres psql -d $DB_NAME -c "SELECT COUNT(*) FROM $TABLE_NAME;"

# Run benchmark queries
echo ""
echo "=== Running Benchmark Queries (Index: $USE_INDEX, Single thread: $SINGLE_THREAD) ==="

TRIES=3

run_query() {
    local query_num=$1
    local query="$2"
    echo ""
    echo "--- Query $query_num ---"
    echo "$query" | head -c 100
    echo "..."

    local parallel_set=""
    if [[ "$SINGLE_THREAD" == "true" ]]; then
        parallel_set="-c \"SET max_parallel_workers_per_gather = 0;\""
    fi

    # First run: print full results
    if [[ "$SINGLE_THREAD" == "true" ]]; then
        sudo -u postgres psql -d $DB_NAME \
            -c "SET max_parallel_workers_per_gather = 0;" \
            -c "\\timing on" \
            -c "$query" 2>&1
    else
        sudo -u postgres psql -d $DB_NAME \
            -c "\\timing on" \
            -c "$query" 2>&1
    fi

    # Subsequent runs: timing only
    for i in $(seq 2 $TRIES); do
        if [[ "$SINGLE_THREAD" == "true" ]]; then
            sudo -u postgres psql -d $DB_NAME \
                -c "SET max_parallel_workers_per_gather = 0;" \
                -c "\\timing on" \
                -c "$query" 2>&1 | grep -E "Time:|rows\)"
        else
            sudo -u postgres psql -d $DB_NAME \
                -c "\\timing on" \
                -c "$query" 2>&1 | grep -E "Time:|rows\)"
        fi
    done
}

# Q1: Top event types
run_query 1 "SELECT data -> 'commit' ->> 'collection' AS event, COUNT(*) as count FROM bluesky GROUP BY event ORDER BY count DESC;"

# Q2: Event types with unique users
run_query 2 "SELECT data -> 'commit' ->> 'collection' AS event, COUNT(*) as count, COUNT(DISTINCT data ->> 'did') AS users FROM bluesky WHERE data ->> 'kind' = 'commit' AND data -> 'commit' ->> 'operation' = 'create' GROUP BY event ORDER BY count DESC;"

# Q3: Event counts by hour
run_query 3 "SELECT data->'commit'->>'collection' AS event, EXTRACT(HOUR FROM TO_TIMESTAMP((data->>'time_us')::BIGINT / 1000000)) AS hour_of_day, COUNT(*) AS count FROM bluesky WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create' AND data->'commit'->>'collection' IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') GROUP BY event, hour_of_day ORDER BY hour_of_day, event;"

# Q4: First 3 users to post
run_query 4 "SELECT data->>'did' AS user_id, MIN( TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 microsecond' * (data->>'time_us')::BIGINT ) AS first_post_ts FROM bluesky WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create' AND data->'commit'->>'collection' = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_ts ASC LIMIT 3;"

# Q5: Top users by activity span
run_query 5 "SELECT data->>'did' AS user_id, EXTRACT(EPOCH FROM ( MAX( TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 microsecond' * (data->>'time_us')::BIGINT ) - MIN( TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 microsecond' * (data->>'time_us')::BIGINT ) )) * 1000 AS activity_span FROM bluesky WHERE data->>'kind' = 'commit' AND data->'commit'->>'operation' = 'create' AND data->'commit'->>'collection' = 'app.bsky.feed.post' GROUP BY user_id ORDER BY activity_span DESC LIMIT 3;"

echo ""
if [[ "$NO_CLEANUP" == "true" ]]; then
    echo "=== Skipping cleanup (--no-cleanup) ==="
    echo "Database '$DB_NAME' kept. To drop manually:"
    echo "  sudo -u postgres psql -c \"DROP DATABASE $DB_NAME;\""
else
    echo "=== Cleanup ==="
    sudo -u postgres psql -c "DROP DATABASE $DB_NAME;"
fi
echo "Done."
