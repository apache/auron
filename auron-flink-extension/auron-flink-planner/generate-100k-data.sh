#!/bin/bash
#
# Generate 100k rows of Parquet test data for Flink + Auron testing
# This version keeps the data for cluster queries
#

set -e

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
cd "$(dirname "$0")/../.."

DATA_DIR="/tmp/flink_auron_100k_$(date +%s)"

echo "=============================================="
echo "Generating 100k Parquet Test Data"
echo "=============================================="
echo ""
echo "Output directory: $DATA_DIR"
echo ""

# Build classpath
CLASSPATH=$(./build/apache-maven-3.9.12/bin/mvn dependency:build-classpath \
  -pl auron-flink-extension/auron-flink-planner -am \
  -Pflink-1.18 -Pscala-2.12 \
  -DincludeScope=test -q -Dmdep.outputFile=/dev/stdout)

CLASSPATH="$CLASSPATH:auron-flink-extension/auron-flink-planner/target/test-classes:auron-flink-extension/auron-flink-planner/target/classes:auron-flink-extension/auron-flink-runtime/target/auron-flink-runtime-7.0.0-SNAPSHOT.jar"

# Create a temp Java file that generates data without cleanup
cat > /tmp/GenerateData.java << 'EOF'
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class GenerateData {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: GenerateData <output_path>");
            System.exit(1);
        }

        String outputPath = args[0];
        System.out.println("Generating 100k rows to: " + outputPath);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create source with 100k rows
        tEnv.executeSql("CREATE TABLE test_source ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'datagen',"
                + "  'number-of-rows' = '100000',"
                + "  'fields.id.kind' = 'sequence',"
                + "  'fields.id.start' = '1',"
                + "  'fields.id.end' = '100000',"
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

        // Write data
        long start = System.currentTimeMillis();
        TableResult result = tEnv.executeSql("INSERT INTO test_sink SELECT * FROM test_source");
        result.await();
        long duration = System.currentTimeMillis() - start;

        System.out.println("✅ Generated 100,000 rows in " + duration + "ms");
        System.out.println("✅ Data location: " + outputPath);
    }
}
EOF

# Compile and run
echo "Compiling generator..."
$JAVA_HOME/bin/javac \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  -cp "$CLASSPATH" \
  /tmp/GenerateData.java

echo ""
echo "Generating data..."
$JAVA_HOME/bin/java \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  -cp "$CLASSPATH:/tmp" \
  GenerateData "$DATA_DIR"

echo ""
echo "=============================================="
echo "Data Generation Complete!"
echo "=============================================="
echo ""
echo "Data location: $DATA_DIR"
echo ""

# Show file info
PARQUET_FILES=$(find "$DATA_DIR" -name "*.parquet" | wc -l | tr -d ' ')
TOTAL_SIZE=$(du -sh "$DATA_DIR" | awk '{print $1}')

echo "Parquet files: $PARQUET_FILES"
echo "Total size: $TOTAL_SIZE"
echo ""

echo "Sample files:"
find "$DATA_DIR" -name "*.parquet" | head -5

echo ""
echo "=============================================="
echo "Next: Query on Flink Cluster with Auron"
echo "=============================================="
echo ""
echo "export FLINK_HOME=/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT"
echo "\$FLINK_HOME/bin/start-cluster.sh"
echo "\$FLINK_HOME/bin/sql-client.sh"
echo ""
echo "Then run:"
echo ""
echo "SET 'table.optimizer.auron.enabled' = 'true';"
echo "SET 'execution.runtime-mode' = 'BATCH';"
echo ""
echo "CREATE TABLE sales ("
echo "    id BIGINT,"
echo "    product STRING,"
echo "    amount DOUBLE,"
echo "    category STRING"
echo ") WITH ("
echo "    'connector' = 'filesystem',"
echo "    'path' = '$DATA_DIR',"
echo "    'format' = 'parquet'"
echo ");"
echo ""
echo "SELECT COUNT(*) FROM sales;"
echo "SELECT * FROM sales WHERE amount > 4000.0 LIMIT 10;"
echo ""
