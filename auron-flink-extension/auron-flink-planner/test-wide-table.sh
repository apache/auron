#!/bin/bash

# Test wide table performance with Auron vs Flink native

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DATA_PATH="/tmp/flink_auron_wide_5000000_1769659081"

echo "========================================"
echo "Wide Table Performance Test"
echo "========================================"
echo ""
echo "Dataset: 5M rows, 50 columns (2.9GB)"
echo "Test: Read 4-6 columns out of 50 (aggressive column pruning)"
echo "Path: $DATA_PATH"
echo ""

# Check if data exists
if [ ! -d "$DATA_PATH" ]; then
    echo "❌ Data not found at $DATA_PATH"
    exit 1
fi

# Check cluster
if ! curl -s http://localhost:8081/overview > /dev/null 2>&1; then
    echo "❌ Flink cluster is not running at localhost:8081"
    exit 1
fi

echo "✅ Data found ($(du -sh $DATA_PATH | cut -f1))"
echo "✅ Flink cluster is running"
echo ""

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

# Compile QueryOnClusterWide
echo "Compiling QueryOnClusterWide..."
javac -cp "target/classes:$(ls /Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/*.jar | tr '\n' ':')" \
    src/main/java/org/apache/auron/flink/planner/QueryOnClusterWide.java \
    -d target/classes/

# Create test JAR
echo "Creating test JAR..."
cd target/classes
jar cf ../wide-table-test.jar org/apache/auron/flink/planner/QueryOnClusterWide.class
cd "$SCRIPT_DIR"

# Copy assembly JAR content
echo "Adding Auron assembly to JAR..."
cd target
unzip -qo /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-assembly/target/auron-flink-assembly-7.0.0-SNAPSHOT.jar -d wide-temp/
cd wide-temp
jar uf ../wide-table-test.jar .
cd ..
rm -rf wide-temp
cd "$SCRIPT_DIR"

echo "✅ Test JAR ready"
echo ""

# Test with Auron
echo "========================================"
echo "Testing WITH Auron"
echo "========================================"
echo ""

/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/bin/flink run \
    -c org.apache.auron.flink.planner.QueryOnClusterWide \
    target/wide-table-test.jar \
    "$DATA_PATH" "true"

echo ""
sleep 2

# Test without Auron
echo "========================================"
echo "Testing WITHOUT Auron (Flink native)"
echo "========================================"
echo ""

/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/bin/flink run \
    -c org.apache.auron.flink.planner.QueryOnClusterWide \
    target/wide-table-test.jar \
    "$DATA_PATH" "false"

echo ""
echo "========================================"
echo "Wide Table Test Complete!"
echo "========================================"
echo ""
echo "This test demonstrates column pruning performance:"
echo "  - Auron's native Parquet reader can skip reading unused columns"
echo "  - Reading 4-6 columns out of 50 total"
echo "  - 2.9GB total data, but only ~240MB relevant columns"
echo ""
