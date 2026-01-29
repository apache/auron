#!/bin/bash
#
# Query Parquet data on Flink cluster with/without Auron
#
# REQUIREMENTS:
# - Flink cluster must be running at localhost:8081
# - Start cluster with: $FLINK_HOME/bin/start-cluster.sh
#

set -e

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
cd "$(dirname "$0")/../.."

# Parse arguments
MODE="both"  # Default: run both with and without Auron

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
echo "Querying Parquet Data on Flink Cluster"
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
    echo "Then verify it's running:"
    echo "  curl http://localhost:8081/overview"
    echo ""
    exit 1
fi
echo "✅ Flink cluster is running"
echo ""

echo "Data path: $DATA_PATH"
echo "Mode: $MODE"
echo ""

# Build classpath
CLASSPATH=$(./build/apache-maven-3.9.12/bin/mvn dependency:build-classpath \
  -pl auron-flink-extension/auron-flink-planner -am \
  -Pflink-1.18 -Pscala-2.12 \
  -DincludeScope=test -q -Dmdep.outputFile=/dev/stdout)

CLASSPATH="$CLASSPATH:auron-flink-extension/auron-flink-planner/target/test-classes:auron-flink-extension/auron-flink-planner/target/classes:auron-flink-extension/auron-flink-runtime/target/auron-flink-runtime-7.0.0-SNAPSHOT.jar"

# Create temp Java file
cat > /tmp/QueryOnCluster.java << 'EOF'
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import java.util.Iterator;
import org.apache.flink.types.Row;

public class QueryOnCluster {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: QueryOnCluster <data_path> <auron_enabled>");
            System.exit(1);
        }

        String dataPath = args[0];
        boolean auronEnabled = Boolean.parseBoolean(args[1]);

        String separator = repeatString("=", 80);

        System.out.println(separator);
        System.out.println("Query Execution " + (auronEnabled ? "WITH Auron" : "WITHOUT Auron"));
        System.out.println(separator);
        System.out.println();

        // CRITICAL: Create TableEnvironment in BATCH mode from the start
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Configure to run on remote Flink cluster (localhost:8081)
        tEnv.getConfig().getConfiguration().setString("execution.target", "remote");
        tEnv.getConfig().getConfiguration().setString("execution.remote.host", "localhost");
        tEnv.getConfig().getConfiguration().setInteger("execution.remote.port", 8081);

        // Configure Auron
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", auronEnabled);

        System.out.println("Configuration:");
        System.out.println("  execution.target = remote (localhost:8081)");
        System.out.println("  execution.runtime-mode = BATCH");
        System.out.println("  table.optimizer.auron.enabled = " + auronEnabled);
        System.out.println("  data.path = " + dataPath);
        System.out.println();

        // Create table
        tEnv.executeSql("CREATE TABLE sales ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + dataPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        System.out.println("Table created successfully");
        System.out.println();

        // Query 1: Count
        System.out.println("Query 1: SELECT COUNT(*) FROM sales");
        long start1 = System.currentTimeMillis();
        TableResult result1 = tEnv.executeSql("SELECT COUNT(*) as total_rows FROM sales");

        // Consume results
        Iterator<Row> iter1 = result1.collect();
        long totalRows = 0;
        if (iter1.hasNext()) {
            Row row = iter1.next();
            totalRows = (Long) row.getField(0);
            System.out.println("  Total rows: " + totalRows);
        }

        long duration1 = System.currentTimeMillis() - start1;
        System.out.println("  ⏱  Duration: " + duration1 + "ms");
        System.out.println();

        // Query 2: High value items
        System.out.println("Query 2: SELECT * FROM sales WHERE amount > 4000.0 LIMIT 10");
        long start2 = System.currentTimeMillis();
        TableResult result2 = tEnv.executeSql("SELECT * FROM sales WHERE amount > 4000.0 LIMIT 10");

        // Consume and display first 5 results
        Iterator<Row> iter2 = result2.collect();
        int count = 0;
        System.out.println("  First 5 results:");
        while (iter2.hasNext() && count < 5) {
            System.out.println("    " + iter2.next());
            count++;
        }
        // Consume remaining results
        while (iter2.hasNext() && count < 10) {
            iter2.next();
            count++;
        }

        long duration2 = System.currentTimeMillis() - start2;
        System.out.println("  ⏱  Duration: " + duration2 + "ms");
        System.out.println();

        // Summary
        System.out.println(separator);
        System.out.println("Summary " + (auronEnabled ? "(WITH Auron)" : "(WITHOUT Auron)"));
        System.out.println(separator);
        System.out.println("  Query 1 (COUNT): " + duration1 + "ms");
        System.out.println("  Query 2 (FILTER): " + duration2 + "ms");
        System.out.println("  Total: " + (duration1 + duration2) + "ms");
        System.out.println(separator);
        System.out.println();
    }

    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
EOF

# Compile
echo "Compiling query program..."
$JAVA_HOME/bin/javac \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  -cp "$CLASSPATH" \
  /tmp/QueryOnCluster.java

echo ""

# Execute based on mode
case "$MODE" in
    auron)
        echo "Executing queries WITH Auron..."
        echo ""
        $JAVA_HOME/bin/java \
          --add-opens=java.base/java.nio=ALL-UNNAMED \
          --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
          --add-opens=java.base/java.lang=ALL-UNNAMED \
          -cp "$CLASSPATH:/tmp" \
          QueryOnCluster "$DATA_PATH" "true"
        ;;
    flink)
        echo "Executing queries WITHOUT Auron (Flink native)..."
        echo ""
        $JAVA_HOME/bin/java \
          --add-opens=java.base/java.nio=ALL-UNNAMED \
          --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
          --add-opens=java.base/java.lang=ALL-UNNAMED \
          -cp "$CLASSPATH:/tmp" \
          QueryOnCluster "$DATA_PATH" "false"
        ;;
    both)
        echo "Executing queries WITH Auron..."
        echo ""
        $JAVA_HOME/bin/java \
          --add-opens=java.base/java.nio=ALL-UNNAMED \
          --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
          --add-opens=java.base/java.lang=ALL-UNNAMED \
          -cp "$CLASSPATH:/tmp" \
          QueryOnCluster "$DATA_PATH" "true" > /tmp/auron_result.txt 2>&1

        cat /tmp/auron_result.txt

        echo ""
        echo "Executing queries WITHOUT Auron (Flink native)..."
        echo ""
        $JAVA_HOME/bin/java \
          --add-opens=java.base/java.nio=ALL-UNNAMED \
          --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
          --add-opens=java.base/java.lang=ALL-UNNAMED \
          -cp "$CLASSPATH:/tmp" \
          QueryOnCluster "$DATA_PATH" "false" > /tmp/flink_result.txt 2>&1

        cat /tmp/flink_result.txt

        # Extract timings and show comparison
        echo ""
        echo "================================================================================"
        echo "Performance Comparison"
        echo "================================================================================"
        AURON_Q1=$(grep "Query 1 (COUNT):" /tmp/auron_result.txt | awk '{print $4}')
        AURON_Q2=$(grep "Query 2 (FILTER):" /tmp/auron_result.txt | awk '{print $4}')
        AURON_TOTAL=$(grep "Total:" /tmp/auron_result.txt | tail -1 | awk '{print $2}')

        FLINK_Q1=$(grep "Query 1 (COUNT):" /tmp/flink_result.txt | awk '{print $4}')
        FLINK_Q2=$(grep "Query 2 (FILTER):" /tmp/flink_result.txt | awk '{print $4}')
        FLINK_TOTAL=$(grep "Total:" /tmp/flink_result.txt | tail -1 | awk '{print $2}')

        printf "%-25s %15s %15s\n" "Query" "WITH Auron" "WITHOUT Auron"
        echo "--------------------------------------------------------------------------------"
        printf "%-25s %15s %15s\n" "Query 1 (COUNT)" "$AURON_Q1" "$FLINK_Q1"
        printf "%-25s %15s %15s\n" "Query 2 (FILTER)" "$AURON_Q2" "$FLINK_Q2"
        echo "--------------------------------------------------------------------------------"
        printf "%-25s %15s %15s\n" "Total" "$AURON_TOTAL" "$FLINK_TOTAL"
        echo "================================================================================"
        echo ""

        # Cleanup temp files
        rm -f /tmp/auron_result.txt /tmp/flink_result.txt
        ;;
esac

echo ""
echo "✅ Query execution completed"
