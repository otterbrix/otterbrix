#!/bin/bash

# Quick test script using local test data for MongoDB JSONBench
# Usage: ./test_local.sh <index|noindex> [num_records] [--no-cleanup]

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <index|noindex> [num_records] [--no-cleanup]"
    echo "  index     - create index on JSON fields"
    echo "  noindex   - no index (full collection scan)"
    echo "  --no-cleanup - keep database after test"
    exit 1
fi

USE_INDEX="$1"
if [[ "$USE_INDEX" != "index" && "$USE_INDEX" != "noindex" ]]; then
    echo "Error: First argument must be 'index' or 'noindex'"
    exit 1
fi

NUM_RECORDS="20000"
NO_CLEANUP=false
for arg in "${@:2}"; do
    case "$arg" in
        --no-cleanup) NO_CLEANUP=true ;;
        [0-9]*)       NUM_RECORDS="$arg" ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OTTERBRIX_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_FILE="$OTTERBRIX_ROOT/JSONBench/file_0001.json"
DB_NAME="bluesky_test"
COLLECTION_NAME="bluesky"

echo "=== MongoDB JSONBench Local Test ==="
echo "Test file: $TEST_FILE"
echo "Database:  $DB_NAME"
echo "Records:   $NUM_RECORDS"
echo "Index:     $USE_INDEX"
echo ""

if [[ ! -f "$TEST_FILE" ]]; then
    echo "Error: Test file not found: $TEST_FILE"
    exit 1
fi

# Drop existing database
echo "=== Dropping existing database ==="
mongosh --quiet --eval "db.getSiblingDB('$DB_NAME').dropDatabase();"

# Create collection
echo "=== Creating collection ==="
mongosh --quiet --eval "
    const db = db.getSiblingDB('$DB_NAME');
    db.createCollection('$COLLECTION_NAME',
        { storageEngine: { wiredTiger: { configString: 'block_compressor=zstd' } } }
    );
"

# Create index if requested
if [[ "$USE_INDEX" == "index" ]]; then
    echo "=== Creating index ==="
    mongosh --quiet --eval "
        const db = db.getSiblingDB('$DB_NAME');
        db.$COLLECTION_NAME.createIndex({
            'kind': 1, 'commit.operation': 1, 'commit.collection': 1, 'did': 1, 'time_us': 1
        });
    "
else
    echo "=== Skipping index creation ==="
fi

# Load data
echo "=== Loading data (first $NUM_RECORDS records) ==="
TEMP_FILE=$(mktemp /tmp/bluesky_local.XXXXXX.json)
head -n "$NUM_RECORDS" "$TEST_FILE" > "$TEMP_FILE"

mongoimport --quiet --db "$DB_NAME" --collection "$COLLECTION_NAME" --file "$TEMP_FILE"
rm -f "$TEMP_FILE"

# Count records
echo ""
echo "=== Record count ==="
mongosh --quiet --eval "print(db.getSiblingDB('$DB_NAME').$COLLECTION_NAME.countDocuments());"

# Tune MongoDB for large aggregations
mongosh --quiet --eval "
    db.adminCommand({ setParameter: 1, internalQueryMaxAddToSetBytes: 1073741824 });
    db.adminCommand({ setParameter: 1, internalQueryPlannerGenerateCoveredWholeIndexScans: true });
" 2>/dev/null

# Run benchmark queries
echo ""
echo "=== Running Benchmark Queries (Index: $USE_INDEX) ==="

TRIES=3

run_query() {
    local query_num=$1
    local query="$2"
    echo ""
    echo "--- Query $query_num ---"
    echo "$query" | head -c 120
    echo "..."

    for i in $(seq 1 $TRIES); do
        mongosh --quiet --eval "
            const db = db.getSiblingDB('$DB_NAME');
            const start = new Date();
            const result = $query;
            if (typeof result.toArray === 'function') { result.toArray(); }
            print('Execution time: ' + (new Date() - start) + 'ms');
        "
    done
}

# Q1: Top event types
run_query 1 "db.$COLLECTION_NAME.aggregate([ { \$group: { _id: '\$commit.collection', count: { \$sum: 1 } } }, { \$sort: { count: -1 } } ])"

# Q2: Event types with unique users
run_query 2 "db.$COLLECTION_NAME.aggregate([ { \$match: { 'kind': 'commit', 'commit.operation': 'create' } }, { \$group: { _id: '\$commit.collection', count: { \$sum: 1 }, users: { \$addToSet: '\$did' } } }, { \$project: { event: '\$_id', count: 1, users: { \$size: '\$users' } } }, { \$sort: { count: -1 } } ])"

# Q3: Event counts by hour
run_query 3 "db.$COLLECTION_NAME.aggregate([ { \$match: { 'kind': 'commit', 'commit.operation': 'create', 'commit.collection': { \$in: ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like'] } } }, { \$project: { _id: 0, event: '\$commit.collection', hour_of_day: { \$hour: { \$toDate: { \$divide: ['\$time_us', 1000] } } } } }, { \$group: { _id: { event: '\$event', hour_of_day: '\$hour_of_day' }, count: { \$sum: 1 } } }, { \$sort: { '_id.hour_of_day': 1, '_id.event': 1 } } ])"

# Q4: First 3 users to post
run_query 4 "db.$COLLECTION_NAME.aggregate([ { \$match: { 'kind': 'commit', 'commit.operation': 'create', 'commit.collection': 'app.bsky.feed.post' } }, { \$project: { _id: 0, user_id: '\$did', timestamp: { \$toDate: { \$divide: ['\$time_us', 1000] } } } }, { \$group: { _id: '\$user_id', first_post_ts: { \$min: '\$timestamp' } } }, { \$sort: { first_post_ts: 1 } }, { \$limit: 3 } ])"

# Q5: Top users by activity span
run_query 5 "db.$COLLECTION_NAME.aggregate([ { \$match: { 'kind': 'commit', 'commit.operation': 'create', 'commit.collection': 'app.bsky.feed.post' } }, { \$project: { _id: 0, user_id: '\$did', timestamp: { \$toDate: { \$divide: ['\$time_us', 1000] } } } }, { \$group: { _id: '\$user_id', min_timestamp: { \$min: '\$timestamp' }, max_timestamp: { \$max: '\$timestamp' } } }, { \$project: { activity_span: { \$dateDiff: { startDate: '\$min_timestamp', endDate: '\$max_timestamp', unit: 'millisecond' } } } }, { \$sort: { activity_span: -1 } }, { \$limit: 3 } ])"

echo ""
if [[ "$NO_CLEANUP" == "true" ]]; then
    echo "=== Skipping cleanup (--no-cleanup) ==="
    echo "Database '$DB_NAME' kept. To drop manually:"
    echo "  mongosh --eval \"db.getSiblingDB('$DB_NAME').dropDatabase();\""
else
    echo "=== Cleanup ==="
    mongosh --quiet --eval "db.getSiblingDB('$DB_NAME').dropDatabase();"
fi
echo "Done."
