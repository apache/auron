#!/bin/bash

# Generate wide table test dataset for Auron performance testing
# This creates a table with 50 columns to test:
# - Column pruning (selecting few columns from many)
# - Vectorized operations across wide rows
# - Realistic data warehouse workloads

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check arguments
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <num_rows>"
    echo "Example: $0 5000000  # Generate 5M rows with 50 columns"
    exit 1
fi

NUM_ROWS=$1
TIMESTAMP=$(date +%s)
DATA_PATH="/tmp/flink_auron_wide_${NUM_ROWS}_${TIMESTAMP}"

echo "========================================"
echo "Generating Wide Table Test Data"
echo "========================================"
echo "Rows: $NUM_ROWS"
echo "Columns: 50 (10 BIGINT, 20 DOUBLE, 20 STRING)"
echo "Output: $DATA_PATH"
echo ""

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

# Create temporary Java program to generate wide table data
cat > /tmp/GenerateWideTestData.java << 'JAVAEOF'
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class GenerateWideTestData {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GenerateWideTestData <num_rows> <output_path>");
            System.exit(1);
        }

        String numRows = args[0];
        String outputPath = args[1];

        System.out.println("Generating wide table with " + numRows + " rows to " + outputPath);
        System.out.println("Schema: 10 BIGINT + 20 DOUBLE + 20 STRING columns");

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Build schema with 50 columns
        StringBuilder sourceSchema = new StringBuilder();
        StringBuilder sinkSchema = new StringBuilder();

        // 10 BIGINT columns (id columns)
        for (int i = 0; i < 10; i++) {
            sourceSchema.append("  id").append(i).append(" BIGINT,\n");
            sinkSchema.append("  id").append(i).append(" BIGINT,\n");
        }

        // 20 DOUBLE columns (metric columns)
        for (int i = 0; i < 20; i++) {
            sourceSchema.append("  metric").append(i).append(" DOUBLE,\n");
            sinkSchema.append("  metric").append(i).append(" DOUBLE,\n");
        }

        // 20 STRING columns (dimension columns)
        for (int i = 0; i < 20; i++) {
            if (i < 19) {
                sourceSchema.append("  dim").append(i).append(" STRING,\n");
                sinkSchema.append("  dim").append(i).append(" STRING,\n");
            } else {
                // Last column without comma
                sourceSchema.append("  dim").append(i).append(" STRING\n");
                sinkSchema.append("  dim").append(i).append(" STRING\n");
            }
        }

        // Build datagen configuration
        StringBuilder datagen = new StringBuilder();
        datagen.append("  'connector' = 'datagen',\n");
        datagen.append("  'number-of-rows' = '").append(numRows).append("',\n");

        // Configure id columns
        for (int i = 0; i < 10; i++) {
            datagen.append("  'fields.id").append(i).append(".kind' = 'sequence',\n");
            datagen.append("  'fields.id").append(i).append(".start' = '").append(i * 1000000).append("',\n");
            datagen.append("  'fields.id").append(i).append(".end' = '").append((i + 1) * 1000000).append("',\n");
        }

        // Configure metric columns
        for (int i = 0; i < 20; i++) {
            datagen.append("  'fields.metric").append(i).append(".min' = '0.0',\n");
            datagen.append("  'fields.metric").append(i).append(".max' = '10000.0',\n");
        }

        // Configure dimension columns
        for (int i = 0; i < 20; i++) {
            if (i < 19) {
                datagen.append("  'fields.dim").append(i).append(".length' = '20',\n");
            } else {
                // Last property without comma
                datagen.append("  'fields.dim").append(i).append(".length' = '20'\n");
            }
        }

        // Create source table
        String sourceTable = "CREATE TABLE test_source (\n" + sourceSchema.toString() + ") WITH (\n" + datagen.toString() + ")";
        tEnv.executeSql(sourceTable);

        // Create sink table
        String sinkTable = "CREATE TABLE test_sink (\n" + sinkSchema.toString() +
            ") WITH (\n" +
            "  'connector' = 'filesystem',\n" +
            "  'path' = '" + outputPath + "',\n" +
            "  'format' = 'parquet'\n" +
            ")";
        tEnv.executeSql(sinkTable);

        System.out.println("Writing data...");
        long start = System.currentTimeMillis();
        TableResult result = tEnv.executeSql("INSERT INTO test_sink SELECT * FROM test_source");
        result.await();
        long duration = System.currentTimeMillis() - start;

        System.out.println("✅ Wide table data generation complete in " + (duration / 1000) + " seconds");
        System.out.println("Data path: " + outputPath);
    }
}
JAVAEOF

# Compile and run
echo "Compiling wide table data generator..."
javac -cp "$SCRIPT_DIR/target/classes:$(ls /Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/*.jar | tr '\n' ':')" \
    /tmp/GenerateWideTestData.java

echo "Generating wide table data..."
java -cp "/tmp:$SCRIPT_DIR/target/classes:$(ls /Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/*.jar | tr '\n' ':')" \
    GenerateWideTestData "$NUM_ROWS" "$DATA_PATH"

# Show results
echo ""
echo "========================================"
echo "Wide Table Test Data Generated"
echo "========================================"
echo "Path: $DATA_PATH"
ls -lh "$DATA_PATH"
echo ""
echo "Total size:"
du -sh "$DATA_PATH"
echo ""
echo "Schema: 50 columns"
echo "  - 10 BIGINT columns (id0-id9)"
echo "  - 20 DOUBLE columns (metric0-metric19)"
echo "  - 20 STRING columns (dim0-dim19)"
echo ""
echo "Example queries to test column pruning:"
echo "  SELECT id0, metric0, metric5 FROM sales WHERE metric0 > 5000.0 LIMIT 100"
echo "  SELECT id0, id1, dim0, dim1 FROM sales LIMIT 100"
echo ""
echo "To test with this data:"
echo "  ./submit-to-cluster.sh $DATA_PATH wide"
echo ""
