#!/bin/bash

# Generate large test dataset for Auron performance testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check arguments
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <num_rows>"
    echo "Example: $0 10000000  # Generate 10M rows"
    exit 1
fi

NUM_ROWS=$1
TIMESTAMP=$(date +%s)
DATA_PATH="/tmp/flink_auron_${NUM_ROWS}_${TIMESTAMP}"

echo "========================================"
echo "Generating Test Data"
echo "========================================"
echo "Rows: $NUM_ROWS"
echo "Output: $DATA_PATH"
echo ""

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

# Create temporary Java program to generate data
cat > /tmp/GenerateTestData.java << 'JAVAEOF'
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class GenerateTestData {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: GenerateTestData <num_rows> <output_path>");
            System.exit(1);
        }

        String numRows = args[0];
        String outputPath = args[1];

        System.out.println("Generating " + numRows + " rows to " + outputPath);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create source with generated data
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

        // Create Parquet sink
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

        System.out.println("Writing data...");
        long start = System.currentTimeMillis();
        TableResult result = tEnv.executeSql("INSERT INTO test_sink SELECT * FROM test_source");
        result.await();
        long duration = System.currentTimeMillis() - start;

        System.out.println("✅ Data generation complete in " + (duration / 1000) + " seconds");
        System.out.println("Data path: " + outputPath);
    }
}
JAVAEOF

# Compile and run
echo "Compiling data generator..."
javac -cp "$SCRIPT_DIR/target/classes:$(ls /Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/*.jar | tr '\n' ':')" \
    /tmp/GenerateTestData.java

echo "Generating data..."
java -cp "/tmp:$SCRIPT_DIR/target/classes:$(ls /Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/*.jar | tr '\n' ':')" \
    GenerateTestData "$NUM_ROWS" "$DATA_PATH"

# Show results
echo ""
echo "========================================"
echo "Test Data Generated"
echo "========================================"
echo "Path: $DATA_PATH"
ls -lh "$DATA_PATH"
echo ""
echo "Total size:"
du -sh "$DATA_PATH"
echo ""
echo "To test with this data:"
echo "  ./submit-to-cluster.sh $DATA_PATH"
echo ""
