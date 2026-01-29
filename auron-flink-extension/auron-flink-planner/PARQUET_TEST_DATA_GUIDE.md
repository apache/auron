# Parquet Test Data Generation for Flink + Auron

## Overview

This guide explains how to generate 100k rows of Parquet test data and query it with Flink + Auron native execution.

## Available Test Scripts

### 1. **run-execution-test.sh** - Full Verification Test

Runs `AuronExecutionVerificationTest` which:
- ✅ Generates 100k rows of test data
- ✅ Executes queries WITH Auron enabled
- ✅ Executes queries WITHOUT Auron (for comparison)
- ✅ Shows Auron native execution logs
- ✅ Cleans up data automatically

**Usage:**
```bash
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner
./run-execution-test.sh
```

**Expected Output:**
```
================================================================================
Flink-Auron Execution Verification Test
================================================================================

Creating test Parquet data...
✅ Test data created at: /tmp/auron_exec_test_1769643088729

================================================================================
EXECUTING Query WITH Auron Enabled
================================================================================
IMPORTANT: Watch for these log messages during execution:
  - 'Auron native execution is available and will be used'
  - 'Converted BatchPhysicalTableSourceScan to Auron native execution'
================================================================================

[INFO] [auron::rt:147] - start executing plan: ParquetExec
[INFO] [datafusion_datasource_parquet::opener:421] - executing parquet scan
[INFO] [auron::rt:188] - task finished

First 5 results:
  +I[1, f3cea32f9056da8592b8, 418.2325654793583]
  +I[2, a72d36bc393c61c592c9, 316.9859135605626]
  ...

✅ Query executed successfully in 1736ms
   Total rows processed: 5+

================================================================================
EXECUTING Query WITHOUT Auron (for comparison)
================================================================================

✅ Query executed successfully in 518ms
   Total rows processed: 5+

✅ Test data cleaned up: /tmp/auron_exec_test_1769643088729

================================================================================
Test Completed
================================================================================

Did you see the Auron conversion log messages above?
If yes: ✅ Auron integration is working!
If no:  ⚠️  Auron conversion may not have occurred
```

**What to Look For:**
- `[INFO] [auron::rt:*]` - Auron native execution logs
- `ParquetExec` execution plans
- Query timing comparison (WITH vs WITHOUT Auron)

### 2. **generate-100k-data.sh** - Persistent Data Generator

Generates 100k rows and keeps the data for Flink cluster queries.

**Usage:**
```bash
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner
./generate-100k-data.sh
```

**Output:**
```
==============================================
Generating 100k Parquet Test Data
==============================================

Output directory: /tmp/flink_auron_100k_1769643200

Generating data...
✅ Generated 100,000 rows in 2341ms
✅ Data location: /tmp/flink_auron_100k_1769643200

Parquet files: 14
Total size: 1.2M

Sample files:
/tmp/flink_auron_100k_1769643200/part-....parquet
...

==============================================
Next: Query on Flink Cluster with Auron
==============================================
```

### 3. **submit-to-cluster.sh** - Submit Jobs to Flink Cluster (⚠️ Has Boundedness Bug)

**⚠️ Currently blocked by Flink schema caching bug - use query-on-cluster.sh instead**

Attempts to submit query jobs to Flink cluster using `flink run` to make them visible in Web UI at http://localhost:8081.

**Known Issue:** Hits the same Flink schema caching bug as SQL Client where filesystem sources are detected as UNBOUNDED even in BATCH mode. This happens because the catalog caches table boundedness information and doesn't update it properly when tables are created in BATCH mode via remote submission.

**Status:** Resolved ClassNotFoundException for PhysicalPlanNode by adding Auron assembly JAR to Flink lib directory, but boundedness detection bug remains.

**Prerequisites:**
```bash
# Start Flink cluster if not already running
export FLINK_HOME=/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT
$FLINK_HOME/bin/start-cluster.sh

# Verify cluster is running
curl http://localhost:8081/overview
```

**Usage:**
```bash
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner

# Run both WITH and WITHOUT Auron (default)
./submit-to-cluster.sh /tmp/flink_auron_100k_1769643247

# Run only WITH Auron
./submit-to-cluster.sh /tmp/flink_auron_100k_1769643247 --mode auron

# Run only WITHOUT Auron (Flink native)
./submit-to-cluster.sh /tmp/flink_auron_100k_1769643247 --mode flink
```

**What it does:**
1. Compiles `QueryOnCluster.java`
2. Creates a cluster test JAR by copying the assembly JAR and adding QueryOnCluster.class
   - This avoids modifying the original assembly build artifact
   - The cluster test JAR is created at `target/auron-flink-cluster-test.jar`
3. Submits the JAR to Flink cluster using `flink run` command
4. Jobs appear in Web UI: http://localhost:8081
5. Shows query results and timing measurements
6. Optionally compares Auron vs Flink native performance

**Output (both mode):**
```
================================================================================
Query Execution WITH Auron
================================================================================

Configuration:
  execution.target = remote (localhost:8081)
  execution.runtime-mode = BATCH
  table.optimizer.auron.enabled = true
  data.path = /tmp/flink_auron_100k_1769643247

Query 1: SELECT COUNT(*) FROM sales
  Total rows: 100000
  ⏱  Duration: 1823ms

Query 2: SELECT * FROM sales WHERE amount > 4000.0 LIMIT 10
  First 5 results:
    +I[1, abc..., 4123.45, CAT-1]
    ...
  ⏱  Duration: 1456ms

================================================================================
Summary (WITH Auron)
================================================================================
  Query 1 (COUNT): 1823ms
  Query 2 (FILTER): 1456ms
  Total: 3279ms
================================================================================

================================================================================
Query Execution WITHOUT Auron
================================================================================

...

  Query 1 (COUNT): 534ms
  Query 2 (FILTER): 412ms
  Total: 946ms
================================================================================

================================================================================
Performance Comparison
================================================================================
Query                     WITH Auron    WITHOUT Auron
--------------------------------------------------------------------------------
Query 1 (COUNT)              1823ms            534ms
Query 2 (FILTER)             1456ms            412ms
--------------------------------------------------------------------------------
Total                        3279ms            946ms
================================================================================
```

**Why This Script?**
- **Proper cluster submission**: Uses `flink run` to submit jobs to the cluster
- **Visible in Web UI**: Jobs appear in Flink Web UI at http://localhost:8081
- **Distributed execution**: Jobs run on cluster TaskManagers, not locally
- **Workaround for SQL Client bug**: SQL Client has a schema caching issue with boundedness detection
- **Performance comparison**: Easily compare Auron vs Flink native execution
- **Timing measurement**: Shows exact query execution times

### 4. **query-on-cluster.sh** - Local Execution (Faster, but not visible in UI)

Runs queries in local/embedded mode. Faster for quick testing but jobs don't appear in Web UI.

**When to use:**
- Quick local testing
- Don't need to see jobs in Web UI
- Faster startup (no job submission overhead)

**Usage:**
```bash
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner
./query-on-cluster.sh /tmp/flink_auron_100k_1769643247 --mode auron
```

**Note:** Despite the name, this runs in embedded mode within the Java process. Use `submit-to-cluster.sh` to actually run on the cluster.

## Querying on Flink Cluster

### Option 1: Use submit-to-cluster.sh (Recommended)

**⭐ Best option to see jobs in Flink Web UI**

After generating data with `generate-100k-data.sh`, submit jobs to the cluster:

```bash
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner
./submit-to-cluster.sh /tmp/flink_auron_100k_<timestamp>
```

This properly submits jobs to the cluster using `flink run`, making them visible in the Web UI at http://localhost:8081.

### Option 2: Manual SQL Client (Has Known Issues)

**⚠️ Warning**: SQL Client has a bug where `SET execution.runtime-mode=BATCH` doesn't properly update schema boundedness detection. You may get "Detected an UNBOUNDED source with execution.runtime-mode set to BATCH" errors. Use Option 1 (query-on-cluster.sh) instead.

If you still want to try SQL Client manually:

### 1. Start Flink Cluster

```bash
export FLINK_HOME=/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT
$FLINK_HOME/bin/start-cluster.sh
```

### 2. Launch SQL Client

```bash
$FLINK_HOME/bin/sql-client.sh
```

### 3. Run Queries with Auron

```sql
-- Enable Auron native execution
SET 'table.optimizer.auron.enabled' = 'true';
SET 'execution.runtime-mode' = 'BATCH';

-- Create table pointing to your data
-- IMPORTANT: Update path to match your generated data directory
CREATE TABLE sales (
    id BIGINT,
    product STRING,
    amount DOUBLE,
    category STRING
) WITH (
    'connector' = 'filesystem',
    'path' = '/tmp/flink_auron_100k_1769643200',  -- <-- UPDATE THIS
    'format' = 'parquet'
);

-- Test queries (Auron accelerates ParquetScan operations)
SELECT COUNT(*) FROM sales;

SELECT * FROM sales WHERE amount > 4000.0 LIMIT 10;

SELECT
    category,
    COUNT(*) as count,
    AVG(amount) as avg_amount,
    MAX(amount) as max_amount
FROM sales
GROUP BY category
ORDER BY count DESC
LIMIT 20;
```

### 4. Verify Auron Execution

**Check Flink Web UI:**
- Open http://localhost:8081
- View running jobs
- Check execution plan

**Check TaskManager Logs:**
```bash
grep -i "auron\|ParquetExec" $FLINK_HOME/log/flink-*-taskexecutor-*.log | tail -20
```

**Look for:**
```
INFO - Auron native execution is available
INFO - Converted BatchPhysicalTableSourceScan to Auron native execution
INFO - initializing auron native environment
INFO - start executing plan: ParquetExec
```

## Test Data Schema

All generated data has this schema:

| Column   | Type   | Description                    |
|----------|--------|--------------------------------|
| id       | BIGINT | Sequential ID (1 to 100,000)   |
| product  | STRING | Random 20-character string     |
| amount   | DOUBLE | Random value (10.0 to 5000.0)  |
| category | STRING | Random 10-character string     |

## Technical Details

### JVM Flags Required

The test scripts include these JVM flags for Java 17 compatibility with Apache Arrow:

```bash
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
```

### Classpath Components

1. **Test dependencies:** Built via Maven dependency:build-classpath
2. **Test classes:** `target/test-classes`
3. **Main classes:** `target/classes`
4. **Auron runtime:** `auron-flink-runtime-7.0.0-SNAPSHOT.jar`

### Why Direct Java Execution?

We bypass Maven's exec:java plugin because:
1. Property resolution issues: `${project.version}` in POMs
2. Reactor context problems: `-am` flag causes exec:java to run from parent module
3. ClassNotFoundException: Missing dependencies in exec:java classpath

Running directly with `java -cp` avoids all these issues.

## Troubleshooting

### Issue: NoClassDefFoundError for Arrow classes

**Solution:** Ensure JVM module access flags are included:
```bash
--add-opens=java.base/java.nio=ALL-UNNAMED
```

### Issue: Missing Auron JNI classes

**Solution:** Verify auron-flink-runtime JAR is in classpath:
```bash
find /Users/vsowrira/git/auron -name "auron-flink-runtime-*.jar"
```

### Issue: Test doesn't show Auron logs

**Solution:**
1. Check that `table.optimizer.auron.enabled = true`
2. Ensure you're using ParquetScan (not VALUES or other sources)
3. Run in BATCH mode, not STREAMING

### Issue: Data not found on cluster

**Solution:**
1. Verify data path exists: `ls /tmp/flink_auron_100k_*`
2. Check path in CREATE TABLE statement matches generated directory
3. Ensure Flink has read permissions to /tmp

## Performance Notes

From test execution:
- **Data Generation:** ~2-3 seconds for 100k rows
- **Query WITH Auron:** ~1700ms (first run, includes JNI initialization)
- **Query WITHOUT Auron:** ~500ms (Flink native execution)

**Note:** Auron's benefits are more apparent with:
- Larger datasets (1M+ rows)
- Complex predicates and filters
- Column pruning and projection pushdown
- Multiple concurrent queries

## Summary

✅ **run-execution-test.sh** - Best for verifying Auron integration works
✅ **generate-100k-data.sh** - Best for creating persistent data for cluster testing
✅ Both scripts handle all classpath and JVM configuration automatically
✅ All you need is Java 17 and a built Auron project
