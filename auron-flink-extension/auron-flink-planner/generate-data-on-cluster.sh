#!/bin/bash

# Generate test data using Flink cluster with parallelism=4

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check arguments
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "Usage: $0 <num_rows> [wide]"
    echo ""
    echo "Examples:"
    echo "  $0 10000000           # Generate 10M rows, 4 columns"
    echo "  $0 10000000 wide      # Generate 10M rows, 50 columns"
    exit 1
fi

NUM_ROWS=$1
IS_WIDE=${2:-""}
TIMESTAMP=$(date +%s)

if [ "$IS_WIDE" = "wide" ]; then
    DATA_PATH="/tmp/flink_auron_wide_${NUM_ROWS}_${TIMESTAMP}"
    NUM_COLS=50
else
    DATA_PATH="/tmp/flink_auron_${NUM_ROWS}_${TIMESTAMP}"
    NUM_COLS=4
fi

echo "========================================"
echo "Generating Test Data on Flink Cluster"
echo "========================================"
echo "Rows: $NUM_ROWS"
echo "Columns: $NUM_COLS"
echo "Parallelism: 4 (using cluster task slots)"
echo "Output: $DATA_PATH"
echo ""

# Check if cluster is running
if ! curl -s http://localhost:8081/overview > /dev/null 2>&1; then
    echo "❌ Flink cluster is not running at localhost:8081"
    echo "Start cluster with: cd /Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT && ./bin/start-cluster.sh"
    exit 1
fi

echo "✅ Flink cluster is running"
echo ""

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

# Create Java program to generate data
if [ "$IS_WIDE" = "wide" ]; then
    # Wide table with 50 columns
    cat > /tmp/GenerateDataOnCluster.java << 'JAVAEOF'
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class GenerateDataOnCluster {
    public static void main(String[] args) throws Exception {
        String numRows = args[0];
        String outputPath = args[1];

        System.out.println("Generating WIDE table: " + numRows + " rows, 50 columns");
        System.out.println("Output: " + outputPath);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Disable Auron for data generation (datagen is not a filesystem source)
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", false);

        // Build schema
        StringBuilder sourceSchema = new StringBuilder();
        StringBuilder sinkSchema = new StringBuilder();

        for (int i = 0; i < 10; i++) {
            sourceSchema.append("  id").append(i).append(" BIGINT,\n");
            sinkSchema.append("  id").append(i).append(" BIGINT,\n");
        }
        for (int i = 0; i < 20; i++) {
            sourceSchema.append("  metric").append(i).append(" DOUBLE,\n");
            sinkSchema.append("  metric").append(i).append(" DOUBLE,\n");
        }
        for (int i = 0; i < 19; i++) {
            sourceSchema.append("  dim").append(i).append(" STRING,\n");
            sinkSchema.append("  dim").append(i).append(" STRING,\n");
        }
        sourceSchema.append("  dim19 STRING\n");
        sinkSchema.append("  dim19 STRING\n");

        // Build datagen
        StringBuilder datagen = new StringBuilder();
        datagen.append("  'connector' = 'datagen',\n");
        datagen.append("  'number-of-rows' = '").append(numRows).append("',\n");
        for (int i = 0; i < 10; i++) {
            datagen.append("  'fields.id").append(i).append(".kind' = 'sequence',\n");
            datagen.append("  'fields.id").append(i).append(".start' = '0',\n");
            datagen.append("  'fields.id").append(i).append(".end' = '").append(numRows).append("',\n");
        }
        for (int i = 0; i < 20; i++) {
            datagen.append("  'fields.metric").append(i).append(".min' = '0.0',\n");
            datagen.append("  'fields.metric").append(i).append(".max' = '10000.0',\n");
        }
        for (int i = 0; i < 19; i++) {
            datagen.append("  'fields.dim").append(i).append(".length' = '20',\n");
        }
        datagen.append("  'fields.dim19.length' = '20'\n");

        tEnv.executeSql("CREATE TABLE test_source (\n" + sourceSchema + ") WITH (\n" + datagen + ")");
        tEnv.executeSql("CREATE TABLE test_sink (\n" + sinkSchema +
            ") WITH ('connector' = 'filesystem', 'path' = '" + outputPath + "', 'format' = 'parquet')");

        System.out.println("Writing data with parallelism=4...");
        long start = System.currentTimeMillis();
        TableResult result = tEnv.executeSql("INSERT INTO test_sink SELECT * FROM test_source");
        result.await();
        long duration = System.currentTimeMillis() - start;

        System.out.println("✅ Generated in " + (duration / 1000.0) + " seconds");
    }
}
JAVAEOF
else
    # Standard 4-column table
    cat > /tmp/GenerateDataOnCluster.java << 'JAVAEOF'
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class GenerateDataOnCluster {
    public static void main(String[] args) throws Exception {
        String numRows = args[0];
        String outputPath = args[1];

        System.out.println("Generating: " + numRows + " rows, 4 columns");
        System.out.println("Output: " + outputPath);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Disable Auron for data generation (datagen is not a filesystem source)
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", false);

        tEnv.executeSql("CREATE TABLE test_source ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'datagen',"
                + "  'number-of-rows' = '" + numRows + "',"
                + "  'fields.id.kind' = 'sequence',"
                + "  'fields.id.start' = '0',"
                + "  'fields.id.end' = '" + numRows + "',"
                + "  'fields.product.length' = '20',"
                + "  'fields.amount.min' = '10.0',"
                + "  'fields.amount.max' = '5000.0',"
                + "  'fields.category.length' = '10'"
                + ")");

        tEnv.executeSql("CREATE TABLE test_sink ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + outputPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        System.out.println("Writing data with parallelism=4...");
        long start = System.currentTimeMillis();
        TableResult result = tEnv.executeSql("INSERT INTO test_sink SELECT * FROM test_source");
        result.await();
        long duration = System.currentTimeMillis() - start;

        System.out.println("✅ Generated in " + (duration / 1000.0) + " seconds");
    }
}
JAVAEOF
fi

# Compile
echo "Compiling data generator..."
javac -cp "$SCRIPT_DIR/target/classes:$(ls /Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/*.jar | tr '\n' ':')" \
    /tmp/GenerateDataOnCluster.java

# Package into JAR with manifest
echo "Creating generator JAR..."
cd /tmp
echo "Main-Class: GenerateDataOnCluster" > manifest.txt
jar cfm data-generator.jar manifest.txt GenerateDataOnCluster.class
cd "$SCRIPT_DIR"

# Submit to cluster
echo "Submitting to Flink cluster..."
/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/bin/flink run \
    /tmp/data-generator.jar "$NUM_ROWS" "$DATA_PATH"

# Show results
echo ""
echo "========================================"
echo "Test Data Generated"
echo "========================================"
echo "Path: $DATA_PATH"
ls -lh "$DATA_PATH" 2>/dev/null || echo "(checking files...)"
echo ""
echo "Total size:"
du -sh "$DATA_PATH" 2>/dev/null || echo "(calculating...)"
echo ""
echo "To test with this data:"
if [ "$IS_WIDE" = "wide" ]; then
    echo "  ./submit-to-cluster.sh $DATA_PATH wide"
else
    echo "  ./submit-to-cluster.sh $DATA_PATH"
fi
echo ""
