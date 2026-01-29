#!/bin/bash

# Monitor data generation jobs and automatically run performance tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

JOB1_ID="5945c72aaabda5cf1e31d41ad6a98fd7"  # 10M rows, 4 columns
JOB2_ID="4249b0197a452ef0e35ec1419478258c"  # 5M rows, 50 columns

DATA_PATH_10M="/tmp/flink_auron_10000000_1769658711"
DATA_PATH_WIDE="/tmp/flink_auron_wide_5000000_1769659081"

echo "========================================"
echo "Monitoring Data Generation Jobs"
echo "========================================"
echo ""
echo "Job 1: 10M rows, 4 columns"
echo "  JobID: $JOB1_ID"
echo "  Path: $DATA_PATH_10M"
echo ""
echo "Job 2: 5M rows, 50 columns"
echo "  JobID: $JOB2_ID"
echo "  Path: $DATA_PATH_WIDE"
echo ""

check_job_status() {
    local job_id=$1
    curl -s "http://localhost:8081/jobs/${job_id}" | grep -o '"state":"[^"]*"' | cut -d'"' -f4
}

get_job_duration() {
    local job_id=$1
    curl -s "http://localhost:8081/jobs/${job_id}" | python3 -c "import sys, json; data=json.load(sys.stdin); print(int(data['duration']/1000))" 2>/dev/null || echo "0"
}

# Monitor jobs
echo "Monitoring jobs (checking every 10 seconds)..."
echo ""

job1_done=false
job2_done=false
start_time=$(date +%s)

while true; do
    job1_status=$(check_job_status "$JOB1_ID")
    job2_status=$(check_job_status "$JOB2_ID")

    current_time=$(date +%s)
    elapsed=$((current_time - start_time))

    # Get durations
    job1_duration=$(get_job_duration "$JOB1_ID")
    job2_duration=$(get_job_duration "$JOB2_ID")

    echo "$(date '+%H:%M:%S') - Elapsed: ${elapsed}s"
    echo "  Job 1 (10M rows): $job1_status (${job1_duration}s)"
    echo "  Job 2 (5M wide):  $job2_status (${job2_duration}s)"

    # Check if Job 1 finished
    if [ "$job1_status" = "FINISHED" ] && [ "$job1_done" = "false" ]; then
        echo ""
        echo "✅ Job 1 FINISHED! (10M rows, 4 columns)"
        du -sh "$DATA_PATH_10M"
        ls -lh "$DATA_PATH_10M"
        job1_done=true
    fi

    # Check if Job 2 finished
    if [ "$job2_status" = "FINISHED" ] && [ "$job2_done" = "false" ]; then
        echo ""
        echo "✅ Job 2 FINISHED! (5M rows, 50 columns)"
        du -sh "$DATA_PATH_WIDE"
        ls -lh "$DATA_PATH_WIDE" | head -5
        job2_done=true
    fi

    # Check if both are done
    if [ "$job1_done" = "true" ] && [ "$job2_done" = "true" ]; then
        echo ""
        echo "========================================"
        echo "All Data Generation Jobs Complete!"
        echo "========================================"
        echo ""
        break
    fi

    # Check for failures
    if [ "$job1_status" = "FAILED" ]; then
        echo ""
        echo "❌ Job 1 FAILED!"
        exit 1
    fi

    if [ "$job2_status" = "FAILED" ]; then
        echo ""
        echo "❌ Job 2 FAILED!"
        exit 1
    fi

    sleep 10
done

# Show data stats
echo "Data Generation Summary:"
echo ""
echo "Dataset 1 (10M rows, 4 columns):"
echo "  Path: $DATA_PATH_10M"
echo "  Size: $(du -sh $DATA_PATH_10M | cut -f1)"
echo "  Files: $(ls -1 $DATA_PATH_10M | wc -l | tr -d ' ')"
echo ""

echo "Dataset 2 (5M rows, 50 columns - WIDE):"
echo "  Path: $DATA_PATH_WIDE"
echo "  Size: $(du -sh $DATA_PATH_WIDE | cut -f1)"
echo "  Files: $(ls -1 $DATA_PATH_WIDE | wc -l | tr -d ' ')"
echo ""

# Now run performance tests
echo "========================================"
echo "Starting Performance Tests"
echo "========================================"
echo ""
echo "This will test Auron vs Flink native performance"
echo "with realistic large datasets."
echo ""

# Test 1: Standard table (10M rows)
echo ""
echo "========================================"
echo "TEST 1: Standard Table (10M rows)"
echo "========================================"
echo ""
echo "Query: SELECT id, product, amount FROM sales WHERE amount > 1000.0 LIMIT 100"
echo ""

./submit-to-cluster.sh "$DATA_PATH_10M"

# Wait a bit between tests
sleep 5

# Test 2: Wide table (5M rows, 50 columns)
echo ""
echo "========================================"
echo "TEST 2: Wide Table (5M rows, 50 columns)"
echo "========================================"
echo ""
echo "This tests aggressive column pruning:"
echo "  Reading 4 columns out of 50 total"
echo ""

# Update QueryOnCluster to handle wide table
echo "Note: Wide table test requires custom query."
echo "Path: $DATA_PATH_WIDE"
echo ""
echo "To test wide table manually, use:"
echo "  Query: SELECT id0, metric0, metric5, dim0 FROM sales WHERE metric0 > 5000.0 LIMIT 100"
echo ""

echo "========================================"
echo "Performance Testing Complete!"
echo "========================================"
echo ""
echo "Summary of results shown above."
echo ""
