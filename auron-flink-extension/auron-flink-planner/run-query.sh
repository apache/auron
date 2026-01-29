#!/bin/bash

# Flexible query runner - run arbitrary SQL against a dataset with Auron vs Flink native
#
# Usage: ./run-query.sh <data_path> <table_schema> <sql_query> [mode]
#
# mode: "both" (default), "auron", "native"

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ "$#" -lt 3 ]; then
    echo "Usage: $0 <data_path> <table_schema> <sql_query> [mode]"
    echo ""
    echo "Arguments:"
    echo "  data_path:     Path to Parquet data"
    echo "  table_schema:  Column definitions (e.g., 'id BIGINT, name STRING, amount DOUBLE')"
    echo "  sql_query:     SQL query to execute"
    echo "  mode:          'both' (default), 'auron', or 'native'"
    echo ""
    echo "Examples:"
    echo "  # Standard table (4 columns)"
    echo "  ./run-query.sh /tmp/flink_auron_10000000_1769658711 \\"
    echo "    'id BIGINT, product STRING, amount DOUBLE, category STRING' \\"
    echo "    'SELECT id, amount FROM sales WHERE amount > 1000 LIMIT 100'"
    echo ""
    echo "  # Wide table (50 columns) - column pruning test"
    echo "  SCHEMA='id0 BIGINT, id1 BIGINT, id2 BIGINT, id3 BIGINT, id4 BIGINT, id5 BIGINT, id6 BIGINT, id7 BIGINT, id8 BIGINT, id9 BIGINT, metric0 DOUBLE, metric1 DOUBLE, metric2 DOUBLE, metric3 DOUBLE, metric4 DOUBLE, metric5 DOUBLE, metric6 DOUBLE, metric7 DOUBLE, metric8 DOUBLE, metric9 DOUBLE, metric10 DOUBLE, metric11 DOUBLE, metric12 DOUBLE, metric13 DOUBLE, metric14 DOUBLE, metric15 DOUBLE, metric16 DOUBLE, metric17 DOUBLE, metric18 DOUBLE, metric19 DOUBLE, dim0 STRING, dim1 STRING, dim2 STRING, dim3 STRING, dim4 STRING, dim5 STRING, dim6 STRING, dim7 STRING, dim8 STRING, dim9 STRING, dim10 STRING, dim11 STRING, dim12 STRING, dim13 STRING, dim14 STRING, dim15 STRING, dim16 STRING, dim17 STRING, dim18 STRING, dim19 STRING'"
    echo "  ./run-query.sh /tmp/flink_auron_wide_5000000_1769659081 \\"
    echo "    \"\$SCHEMA\" \\"
    echo "    'SELECT id0, metric0, metric5, dim0 FROM sales WHERE metric0 > 5000 LIMIT 100'"
    echo ""
    exit 1
fi

DATA_PATH="$1"
TABLE_SCHEMA="$2"
SQL_QUERY="$3"
MODE="${4:-both}"

# Validate mode
if [[ ! "$MODE" =~ ^(both|auron|native)$ ]]; then
    echo "❌ Invalid mode: $MODE. Must be 'both', 'auron', or 'native'"
    exit 1
fi

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

echo "========================================"
echo "Flexible Query Runner"
echo "========================================"
echo ""
echo "Data: $DATA_PATH ($(du -sh $DATA_PATH | cut -f1))"
echo "Schema: $TABLE_SCHEMA"
echo "Query: $SQL_QUERY"
echo "Mode: $MODE"
echo ""

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

# Compile FlexibleQueryRunner if needed
JAR_FILE="target/flexible-query-runner.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "Compiling FlexibleQueryRunner..."
    javac -cp "target/classes:$(ls /Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/*.jar | tr '\n' ':')" \
        src/main/java/org/apache/auron/flink/planner/FlexibleQueryRunner.java \
        -d target/classes/

    # Create JAR with manifest
    echo "Creating JAR..."
    cd target/classes
    echo "Main-Class: org.apache.auron.flink.planner.FlexibleQueryRunner" > manifest.txt
    jar cfm ../flexible-query-runner-temp.jar manifest.txt org/apache/auron/flink/planner/FlexibleQueryRunner.class
    cd "$SCRIPT_DIR"

    # Add Auron assembly
    echo "Adding Auron assembly..."
    cd target
    # Extract both JARs
    mkdir -p flex-temp
    cd flex-temp
    unzip -qo ../flexible-query-runner-temp.jar
    unzip -qo /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-assembly/target/auron-flink-assembly-7.0.0-SNAPSHOT.jar
    # Recreate JAR with proper manifest
    jar cfm ../flexible-query-runner.jar META-INF/MANIFEST.MF .
    cd ..
    rm -rf flex-temp flexible-query-runner-temp.jar
    cd "$SCRIPT_DIR"

    echo "✅ JAR ready"
    echo ""
fi

FLINK_BIN="/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/bin/flink"

# Run with Auron
if [ "$MODE" = "both" ] || [ "$MODE" = "auron" ]; then
    echo "========================================"
    echo "Testing WITH Auron"
    echo "========================================"
    echo ""

    $FLINK_BIN run \
        -c org.apache.auron.flink.planner.FlexibleQueryRunner \
        "$JAR_FILE" \
        "$DATA_PATH" "true" "$TABLE_SCHEMA" "$SQL_QUERY"

    echo ""
fi

# Wait between tests
if [ "$MODE" = "both" ]; then
    sleep 2
fi

# Run without Auron
if [ "$MODE" = "both" ] || [ "$MODE" = "native" ]; then
    echo "========================================"
    echo "Testing WITHOUT Auron (Flink native)"
    echo "========================================"
    echo ""

    $FLINK_BIN run \
        -c org.apache.auron.flink.planner.FlexibleQueryRunner \
        "$JAR_FILE" \
        "$DATA_PATH" "false" "$TABLE_SCHEMA" "$SQL_QUERY"

    echo ""
fi

echo "========================================"
echo "Query Testing Complete!"
echo "========================================"
echo ""
