# Flink SQL Quick Start with Auron

This guide shows how to run SQL queries on a local Flink cluster with Auron integration.

## Prerequisites

Ensure you've deployed the Auron JARs to your Flink cluster (see [FLINK-DEPLOYMENT.md](FLINK-DEPLOYMENT.md)).

## Starting Flink Cluster

### 1. Start Flink Standalone Cluster

```bash
# Set FLINK_HOME to the correct Flink distribution
# For local Flink build with Auron integration:
export FLINK_HOME=/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT

# Or adjust path as needed for your environment:
# export FLINK_HOME=/path/to/flink

# Start cluster
$FLINK_HOME/bin/start-cluster.sh

# Verify cluster is running
$FLINK_HOME/bin/flink list
```

You should see:
```
No running jobs.
```

Check the Web UI: http://localhost:8081

### 2. Start Flink SQL Client

```bash
$FLINK_HOME/bin/sql-client.sh
```

You'll see the Flink SQL prompt:
```
Flink SQL>
```

## Configure Auron in SQL Client

### Option 1: Set in SQL Session

```sql
-- Enable Auron native execution
SET 'table.optimizer.auron.enabled' = 'true';

-- Set batch execution mode (required for Auron)
SET 'execution.runtime-mode' = 'BATCH';

-- Set parallelism (optional, default is number of cores)
SET 'parallelism.default' = '4';
```

### Option 2: Configure in flink-conf.yaml

Edit `$FLINK_HOME/conf/flink-conf.yaml`:
```yaml
table.optimizer.auron.enabled: true
execution.runtime-mode: BATCH
parallelism.default: 4
```

Then restart the cluster.

## Running SQL Queries

### Example 1: Simple Query with Generated Data

This example generates test data on-the-fly:

```sql
-- Create table with generated data
CREATE TABLE test_source (
    id BIGINT,
    name STRING,
    amount DOUBLE,
    category INT
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '10000',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '10000',
    'fields.name.length' = '20',
    'fields.amount.min' = '10.0',
    'fields.amount.max' = '500.0',
    'fields.category.min' = '1',
    'fields.category.max' = '10'
);

-- Query the data (this runs in memory, NOT using Auron)
SELECT category, COUNT(*) as cnt, AVG(amount) as avg_amount
FROM test_source
GROUP BY category
ORDER BY category;
```

**Note**: Datagen connector doesn't trigger Auron. To use Auron, you need Parquet files.

### Example 2: Query with Parquet Files (Uses Auron)

This is where Auron accelerates execution:

#### Step 1: Create Test Parquet Data

```sql
-- Create source with generated data
CREATE TABLE test_source (
    id BIGINT,
    name STRING,
    amount DOUBLE,
    category INT
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '10000',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '10000',
    'fields.name.length' = '20',
    'fields.amount.min' = '10.0',
    'fields.amount.max' = '500.0',
    'fields.category.min' = '1',
    'fields.category.max' = '10'
);

-- Create Parquet sink
CREATE TABLE test_parquet (
    id BIGINT,
    name STRING,
    amount DOUBLE,
    category INT
) WITH (
    'connector' = 'filesystem',
    'path' = '/tmp/flink_test_data',
    'format' = 'parquet'
);

-- Write data to Parquet
INSERT INTO test_parquet SELECT * FROM test_source;
```

#### Step 2: Query Parquet Data (Auron Acceleration)

```sql
-- Create table pointing to Parquet files
CREATE TABLE sales (
    id BIGINT,
    name STRING,
    amount DOUBLE,
    category INT
) WITH (
    'connector' = 'filesystem',
    'path' = '/tmp/flink_test_data',
    'format' = 'parquet'
);

-- Enable Auron (if not already enabled)
SET 'table.optimizer.auron.enabled' = 'true';
SET 'execution.runtime-mode' = 'BATCH';

-- Query 1: Full scan with projection (Auron: ParquetScan + Projection)
SELECT id, name, amount FROM sales LIMIT 10;

-- Query 2: Filter + Projection (Auron: ParquetScan + Filter + Projection)
SELECT id, name, amount
FROM sales
WHERE amount > 100.0
LIMIT 10;

-- Query 3: Hybrid execution (Auron: ParquetScan, Flink: Aggregation)
SELECT category, COUNT(*) as row_count,
       SUM(amount) as total,
       AVG(amount) as average
FROM sales
GROUP BY category
ORDER BY category;

-- Query 4: Complex filter (Auron: ParquetScan + Filter)
SELECT id, name, amount, category
FROM sales
WHERE amount > 100.0 AND category IN (1, 2, 3)
LIMIT 20;
```

### Example 3: Real-World Use Case

```sql
-- Create external Parquet table (pointing to your data)
CREATE TABLE transactions (
    transaction_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    amount DOUBLE,
    transaction_date DATE,
    store_id INT
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///path/to/your/parquet/files',
    'format' = 'parquet'
);

-- Enable Auron
SET 'table.optimizer.auron.enabled' = 'true';
SET 'execution.runtime-mode' = 'BATCH';

-- Analytics queries (Auron accelerates the scan, Flink does aggregations)

-- Daily sales by store
SELECT
    transaction_date,
    store_id,
    COUNT(*) as num_transactions,
    SUM(amount) as total_sales,
    AVG(amount) as avg_transaction
FROM transactions
WHERE transaction_date >= DATE '2024-01-01'
GROUP BY transaction_date, store_id
ORDER BY transaction_date, store_id;

-- Top customers by spend
SELECT
    customer_id,
    COUNT(*) as num_purchases,
    SUM(amount) as total_spend
FROM transactions
WHERE amount > 50.0
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 100;
```

## Verifying Auron is Working

### Check TaskManager Logs

```bash
# Watch logs in real-time
tail -f $FLINK_HOME/log/flink-*-taskexecutor-*.log | grep -i auron

# Search for Auron initialization
grep "initializing auron native environment" $FLINK_HOME/log/flink-*-taskexecutor-*.log

# Search for Auron execution
grep "start executing plan" $FLINK_HOME/log/flink-*-taskexecutor-*.log
```

Expected output:
```
------ initializing auron native environment ------
[INFO] [auron::exec:70] - initializing JNI bridge
[INFO] [auron_jni_bridge::jni_bridge:491] - ==> FLINK MODE: Spark/Scala classes will be skipped
[INFO] [auron::rt:147] - start executing plan:
ParquetExec: limit=None, file_group=[...]
[INFO] [datafusion_datasource_parquet::opener:421] - executing parquet scan with adaptive batch size: 8192
[INFO] [auron::rt:188] - task finished
```

### Check Query Plan

```sql
-- Use EXPLAIN to see execution plan
EXPLAIN PLAN FOR
SELECT id, name, amount
FROM sales
WHERE amount > 100.0;
```

Look for Auron-related operators in the plan output.

## Running SQL Files

You can also run SQL from files:

### Create SQL Script

Create `test_query.sql`:
```sql
-- Enable Auron
SET 'table.optimizer.auron.enabled' = 'true';
SET 'execution.runtime-mode' = 'BATCH';

-- Create table
CREATE TABLE sales (
    id BIGINT,
    amount DOUBLE,
    category INT
) WITH (
    'connector' = 'filesystem',
    'path' = '/tmp/flink_test_data',
    'format' = 'parquet'
);

-- Run query
SELECT category, COUNT(*) as cnt, AVG(amount) as avg_amount
FROM sales
GROUP BY category
ORDER BY category;
```

### Execute SQL File

```bash
# From SQL client
Flink SQL> SOURCE '/path/to/test_query.sql';

# Or directly from command line
$FLINK_HOME/bin/sql-client.sh -f /path/to/test_query.sql
```

## Programmatic SQL Execution (Java)

You can also run SQL programmatically:

```java
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class AuronSQLExample {
    public static void main(String[] args) {
        // Create batch table environment
        EnvironmentSettings settings =
            EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Enable Auron
        tEnv.getConfig().getConfiguration()
            .setBoolean("table.optimizer.auron.enabled", true);

        // Set parallelism
        tEnv.getConfig().getConfiguration()
            .setInteger("table.exec.resource.default-parallelism", 4);

        // Create table
        tEnv.executeSql("CREATE TABLE sales ("
            + "  id BIGINT,"
            + "  amount DOUBLE,"
            + "  category INT"
            + ") WITH ("
            + "  'connector' = 'filesystem',"
            + "  'path' = '/tmp/flink_test_data',"
            + "  'format' = 'parquet'"
            + ")");

        // Run query
        TableResult result = tEnv.executeSql(
            "SELECT category, COUNT(*) as cnt, AVG(amount) as avg "
            + "FROM sales GROUP BY category ORDER BY category"
        );

        // Print results
        result.print();
    }
}
```

Compile and run:
```bash
javac -cp "$FLINK_HOME/lib/*" AuronSQLExample.java
java -cp ".:$FLINK_HOME/lib/*" AuronSQLExample
```

## Quick Test Script

Here's a complete script to test Auron integration:

```bash
#!/bin/bash

FLINK_HOME=/path/to/flink

# Start Flink
$FLINK_HOME/bin/start-cluster.sh
sleep 5

# Create test script
cat > /tmp/auron_test.sql <<'EOF'
-- Configure Auron
SET 'table.optimizer.auron.enabled' = 'true';
SET 'execution.runtime-mode' = 'BATCH';
SET 'parallelism.default' = '4';

-- Create source with generated data
CREATE TABLE test_source (
    id BIGINT,
    name STRING,
    amount DOUBLE,
    category INT
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '10000',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '10000'
);

-- Create Parquet sink
CREATE TABLE test_parquet (
    id BIGINT,
    name STRING,
    amount DOUBLE,
    category INT
) WITH (
    'connector' = 'filesystem',
    'path' = '/tmp/auron_test_data',
    'format' = 'parquet'
);

-- Write test data
INSERT INTO test_parquet SELECT * FROM test_source;

-- Create table for querying
CREATE TABLE sales (
    id BIGINT,
    name STRING,
    amount DOUBLE,
    category INT
) WITH (
    'connector' = 'filesystem',
    'path' = '/tmp/auron_test_data',
    'format' = 'parquet'
);

-- Test queries
SELECT COUNT(*) as total_rows FROM sales;

SELECT category, COUNT(*) as cnt, AVG(amount) as avg_amount
FROM sales
GROUP BY category
ORDER BY category;

SELECT id, name, amount FROM sales WHERE amount > 400.0 LIMIT 10;
EOF

# Run test
echo "Running Auron test queries..."
$FLINK_HOME/bin/sql-client.sh -f /tmp/auron_test.sql

# Check logs
echo ""
echo "Checking for Auron execution in logs..."
grep -i "initializing auron" $FLINK_HOME/log/flink-*-taskexecutor-*.log | tail -5

# Stop Flink
$FLINK_HOME/bin/stop-cluster.sh
```

## Supported Query Patterns

Auron accelerates these patterns:

✅ **Parquet Scan**
```sql
SELECT * FROM parquet_table;
```

✅ **Parquet Scan + Projection**
```sql
SELECT col1, col2 FROM parquet_table;
```

✅ **Parquet Scan + Filter**
```sql
SELECT * FROM parquet_table WHERE col1 > 100;
```

✅ **Parquet Scan + Filter + Projection**
```sql
SELECT col1, col2 FROM parquet_table WHERE col1 > 100;
```

✅ **Parquet Scan + Limit**
```sql
SELECT * FROM parquet_table LIMIT 100;
```

✅ **Hybrid: Auron Scan + Flink Aggregation**
```sql
SELECT category, COUNT(*), AVG(amount)
FROM parquet_table
GROUP BY category;
```

## Troubleshooting

### Issue: Queries run but don't show Auron logs

**Check**:
1. Verify Auron is enabled:
   ```sql
   SHOW CREATE TABLE your_table;
   -- Should use 'format' = 'parquet'

   SET 'table.optimizer.auron.enabled';
   -- Should return 'true'

   SET 'execution.runtime-mode';
   -- Should return 'BATCH'
   ```

2. Ensure you're querying Parquet tables (not datagen or other connectors)

3. Check TaskManager logs for errors:
   ```bash
   grep -i error $FLINK_HOME/log/flink-*-taskexecutor-*.log
   ```

### Issue: Native library not found

**Error**: `UnsatisfiedLinkError: no auron in java.library.path`

**Fix**: Check assembly JAR contains native library:
```bash
jar tf $FLINK_HOME/lib/auron-flink-assembly-*.jar | grep libauron
```

Should show: `libauron.dylib` (macOS) or `libauron.so` (Linux)

### Issue: Out of memory errors

**Fix**: Increase TaskManager memory in `flink-conf.yaml`:
```yaml
taskmanager.memory.task.heap.size: 2048m
taskmanager.memory.managed.size: 1024m
```

## Performance Tips

1. **Increase parallelism** for large datasets:
   ```sql
   SET 'parallelism.default' = '8';
   ```

2. **Use columnar format**: Auron works best with Parquet

3. **Filter pushdown**: Auron pushes filters to Parquet reader for better performance

4. **Partition your data**: Use partitioned Parquet files for better parallel processing

5. **Monitor execution**: Watch logs to ensure Auron is being used:
   ```bash
   tail -f $FLINK_HOME/log/flink-*-taskexecutor-*.log | grep "ParquetExec"
   ```

## Next Steps

- For deployment to production clusters: [FLINK-DEPLOYMENT.md](FLINK-DEPLOYMENT.md)
- For building and testing: [BUILD-GUIDE.md](BUILD-GUIDE.md)
- For quick examples: [QUICKSTART-FLINK.md](QUICKSTART-FLINK.md)
