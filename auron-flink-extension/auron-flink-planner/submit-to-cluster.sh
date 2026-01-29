#!/bin/bash
#
# Submit Parquet query job to Flink cluster
#
# This script properly submits jobs to the Flink cluster so they appear in the Web UI
#

set -e

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
export FLINK_HOME=/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT

cd "$(dirname "$0")/../.."

# Parse arguments
MODE="both"

usage() {
    echo "Usage: $0 <data_path> [--mode MODE]"
    echo ""
    echo "Arguments:"
    echo "  data_path          Path to Parquet data directory"
    echo ""
    echo "Options:"
    echo "  --mode MODE        Execution mode: 'both', 'auron', 'flink' (default: both)"
    echo ""
    echo "Examples:"
    echo "  $0 /tmp/flink_auron_100k_1769643247"
    echo "  $0 /tmp/flink_auron_100k_1769643247 --mode auron"
    echo "  $0 /tmp/flink_auron_100k_1769643247 --mode flink"
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

DATA_PATH="$1"
shift

while [ $# -gt 0 ]; do
    case "$1" in
        --mode)
            MODE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate mode
if [ "$MODE" != "both" ] && [ "$MODE" != "auron" ] && [ "$MODE" != "flink" ]; then
    echo "Error: Invalid mode '$MODE'. Must be 'both', 'auron', or 'flink'."
    exit 1
fi

echo "=============================================="
echo "Submitting Query Job to Flink Cluster"
echo "=============================================="
echo ""

# Check if Flink cluster is running
echo "Checking if Flink cluster is running at localhost:8081..."
if ! curl -s http://localhost:8081/overview > /dev/null 2>&1; then
    echo ""
    echo "ERROR: Flink cluster is not running at localhost:8081"
    echo ""
    echo "Please start the Flink cluster first:"
    echo "  export FLINK_HOME=/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT"
    echo "  \$FLINK_HOME/bin/start-cluster.sh"
    echo ""
    exit 1
fi
echo "✅ Flink cluster is running"
echo ""

echo "Data path: $DATA_PATH"
echo "Mode: $MODE"
echo ""

# Build cluster test JAR
echo "Preparing job JAR..."

# Compile QueryOnCluster if needed
if [ ! -f "auron-flink-extension/auron-flink-planner/target/classes/org/apache/auron/flink/planner/QueryOnCluster.class" ]; then
    echo "Compiling QueryOnCluster.java..."
    $JAVA_HOME/bin/javac \
      -cp "$(./build/apache-maven-3.9.12/bin/mvn dependency:build-classpath -pl auron-flink-extension/auron-flink-planner -am -Pflink-1.18 -Pscala-2.12 -q -Dmdep.outputFile=/dev/stdout)" \
      -d auron-flink-extension/auron-flink-planner/target/classes \
      auron-flink-extension/auron-flink-planner/src/main/java/org/apache/auron/flink/planner/QueryOnCluster.java
fi

# Use assembly JAR (contains all Auron dependencies)
ASSEMBLY_JAR="auron-flink-extension/auron-flink-assembly/target/auron-flink-assembly-7.0.0-SNAPSHOT.jar"

# Check if assembly JAR exists
if [ ! -f "$ASSEMBLY_JAR" ]; then
    echo "Assembly JAR not found. Building assembly..."
    ./build/apache-maven-3.9.12/bin/mvn package \
      -pl auron-flink-extension/auron-flink-assembly -am \
      -Pflink-1.18 -Pscala-2.12 -DskipTests -Drat.skip=true -q || {
        echo "ERROR: Failed to build assembly"
        exit 1
      }
fi

# Create cluster test JAR by copying assembly and adding QueryOnCluster
CLUSTER_TEST_DIR="auron-flink-extension/auron-flink-planner/target"
mkdir -p "$CLUSTER_TEST_DIR"
JAR_PATH="$CLUSTER_TEST_DIR/auron-flink-cluster-test.jar"

echo "Creating cluster test JAR (assembly + QueryOnCluster)..."
cp "$ASSEMBLY_JAR" "$JAR_PATH"

# Add QueryOnCluster.class
cd auron-flink-extension/auron-flink-planner
jar uf target/auron-flink-cluster-test.jar \
  -C target/classes org/apache/auron/flink/planner/QueryOnCluster.class
cd ../..

# Verify
if ! jar tf "$JAR_PATH" | grep -q "org/apache/auron/flink/planner/QueryOnCluster.class"; then
    echo "ERROR: QueryOnCluster.class not found in JAR"
    exit 1
fi

# Show JAR info
JAR_SIZE=$(du -h "$JAR_PATH" | cut -f1)
echo "✅ Cluster test JAR ready: $JAR_PATH"
echo "✅ JAR size: $JAR_SIZE"
echo ""

# Submit jobs based on mode
submit_job() {
    local AURON_ENABLED=$1
    local MODE_NAME=$2

    echo "=============================================="
    echo "Submitting job: $MODE_NAME"
    echo "=============================================="
    echo ""
    echo "Job will appear in Flink Web UI: http://localhost:8081"
    echo ""

    # Submit job to cluster
    # Use child-first classloading (default) to prioritize our JAR
    $FLINK_HOME/bin/flink run \
        -c org.apache.auron.flink.planner.QueryOnCluster \
        "$JAR_PATH" \
        "$DATA_PATH" \
        "$AURON_ENABLED"

    echo ""
    echo "✅ Job completed: $MODE_NAME"
    echo ""
}

case "$MODE" in
    auron)
        submit_job "true" "WITH Auron"
        ;;
    flink)
        submit_job "false" "WITHOUT Auron (Flink native)"
        ;;
    both)
        submit_job "true" "WITH Auron" | tee /tmp/auron_cluster_result.txt
        echo ""
        submit_job "false" "WITHOUT Auron (Flink native)" | tee /tmp/flink_cluster_result.txt

        # Extract and compare timings
        echo ""
        echo "================================================================================"
        echo "Performance Comparison"
        echo "================================================================================"

        AURON_Q1=$(grep "Query 1 (COUNT):" /tmp/auron_cluster_result.txt | awk '{print $4}')
        AURON_Q2=$(grep "Query 2 (FILTER):" /tmp/auron_cluster_result.txt | awk '{print $4}')
        AURON_TOTAL=$(grep "Total:" /tmp/auron_cluster_result.txt | tail -1 | awk '{print $2}')

        FLINK_Q1=$(grep "Query 1 (COUNT):" /tmp/flink_cluster_result.txt | awk '{print $4}')
        FLINK_Q2=$(grep "Query 2 (FILTER):" /tmp/flink_cluster_result.txt | awk '{print $4}')
        FLINK_TOTAL=$(grep "Total:" /tmp/flink_cluster_result.txt | tail -1 | awk '{print $2}')

        printf "%-25s %15s %15s\n" "Query" "WITH Auron" "WITHOUT Auron"
        echo "--------------------------------------------------------------------------------"
        printf "%-25s %15s %15s\n" "Query 1 (COUNT)" "$AURON_Q1" "$FLINK_Q1"
        printf "%-25s %15s %15s\n" "Query 2 (FILTER)" "$AURON_Q2" "$FLINK_Q2"
        echo "--------------------------------------------------------------------------------"
        printf "%-25s %15s %15s\n" "Total" "$AURON_TOTAL" "$FLINK_TOTAL"
        echo "================================================================================"

        rm -f /tmp/auron_cluster_result.txt /tmp/flink_cluster_result.txt
        ;;
esac

echo ""
echo "✅ All jobs completed"
echo ""
echo "View job history in Flink Web UI: http://localhost:8081/#/job/completed"
