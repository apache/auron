# Verifying Automatic Flink-to-Auron Conversion

This guide shows how to verify that Flink SQL queries are being automatically converted to Auron native execution.

## Method 1: Check Execution Plan with EXPLAIN

The most straightforward way is to use Flink's `EXPLAIN` command to see the execution plan.

### Setup

```java
Configuration config = new Configuration();
config.setBoolean("table.optimizer.auron.enabled", true);
config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// Create a Parquet table
tEnv.executeSql(
    "CREATE TABLE sales (" +
    "  id BIGINT," +
    "  product STRING," +
    "  amount DOUBLE," +
    "  sale_date DATE" +
    ") WITH (" +
    "  'connector' = 'filesystem'," +
    "  'path' = 'file:///path/to/sales.parquet'," +
    "  'format' = 'parquet'" +
    ")"
);
```

### Verify with EXPLAIN

```java
// WITHOUT Auron (disabled)
config.setBoolean("table.optimizer.auron.enabled", false);
String planWithoutAuron = tEnv.explainSql(
    "SELECT id, product FROM sales WHERE amount > 100"
);
System.out.println("=== PLAN WITHOUT AURON ===");
System.out.println(planWithoutAuron);

// WITH Auron (enabled)
config.setBoolean("table.optimizer.auron.enabled", true);
String planWithAuron = tEnv.explainSql(
    "SELECT id, product FROM sales WHERE amount > 100"
);
System.out.println("\n=== PLAN WITH AURON ===");
System.out.println(planWithAuron);
```

### Expected Output

**Without Auron:**
```
== Optimized Execution Plan ==
Calc(select=[id, product], where=[>(amount, 100)])
+- TableSourceScan(table=[[default_catalog, default_database, sales]],
                   fields=[id, product, amount, sale_date])
```

**With Auron (EXPECTED):**
```
== Optimized Execution Plan ==
AuronBatchExecNode[Calc(select=[id, product], where=[>(amount, 100)])
  +- TableSourceScan(table=[[default_catalog, default_database, sales]])]
```

If you see `AuronBatchExecNode` or `Auron[...]` in the plan, the conversion is working!

## Method 2: Check Logs

The `AuronExecNodeGraphProcessor` logs detailed information during conversion.

### Enable Debug Logging

```java
// In log4j2.properties or log4j.properties
logger.auron.name = org.apache.flink.table.planner.plan.nodes.exec.processor.AuronExecNodeGraphProcessor
logger.auron.level = INFO

logger.auronConvert.name = org.apache.auron.flink.planner
logger.auronConvert.level = INFO
```

Or programmatically:
```java
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;

Configurator.setLevel("org.apache.flink.table.planner.plan.nodes.exec.processor.AuronExecNodeGraphProcessor", Level.INFO);
Configurator.setLevel("org.apache.auron.flink.planner", Level.INFO);
```

### Expected Log Messages

When conversion happens, you should see:

```
INFO  AuronExecNodeGraphProcessor - Auron native execution is available and will be used for supported operators
INFO  AuronExecNodeGraphProcessor - Converted BatchExecCalc to Auron native execution: Calc(select=[id, product], where=[>(amount, 100)])
INFO  AuronExecNodeConverter - Converting Flink ExecNode to Auron plan: Calc(select=[id, product], where=[>(amount, 100)])
INFO  AuronExecNodeConverter - Converting Calc + Scan pattern to Auron
INFO  AuronExecNodeConverter - Extracted 1 file paths from table source
INFO  AuronTransformationFactory - Creating Auron transformation for plan type: PROJECTION
INFO  AuronTransformationFactory - Created Auron transformation with parallelism 4 for plan: PROJECTION
```

If Auron is **NOT available** on classpath:
```
WARN  AuronExecNodeGraphProcessor - Auron native execution is enabled in configuration but auron-flink-extension library is not available on the classpath. Falling back to standard Flink execution.
```

## Method 3: Programmatic Verification Test

Create a test that verifies the conversion programmatically.

```java
import org.apache.flink.table.api.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class AuronAutoConversionVerificationTest {

    @Test
    public void testAutomaticConversionToAuron() throws Exception {
        // Setup with Auron enabled
        Configuration config = new Configuration();
        config.setBoolean("table.optimizer.auron.enabled", true);
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Create test table
        tEnv.executeSql(
            "CREATE TABLE test_sales (" +
            "  id BIGINT," +
            "  product STRING," +
            "  amount DOUBLE" +
            ") WITH (" +
            "  'connector' = 'filesystem'," +
            "  'path' = 'file:///tmp/test_sales.parquet'," +
            "  'format' = 'parquet'" +
            ")"
        );

        // Get execution plan
        String plan = tEnv.explainSql(
            "SELECT id, product FROM test_sales WHERE amount > 100",
            ExplainDetail.JSON_EXECUTION_PLAN
        );

        System.out.println("Execution Plan:");
        System.out.println(plan);

        // Verify Auron is in the plan
        boolean hasAuron = plan.contains("Auron") ||
                          plan.contains("AuronBatchExecNode") ||
                          plan.contains("AuronNativeScan");

        if (hasAuron) {
            System.out.println("✅ SUCCESS: Query is using Auron native execution!");
        } else {
            System.out.println("❌ FAIL: Query is NOT using Auron native execution");
            System.out.println("Check that:");
            System.out.println("  1. table.optimizer.auron.enabled = true");
            System.out.println("  2. auron-flink-extension JAR is on classpath");
            System.out.println("  3. Flink planner was compiled with Auron support");
        }

        assertTrue(hasAuron, "Expected Auron to be used in execution plan");
    }

    @Test
    public void testComparisonWithAndWithoutAuron() throws Exception {
        // Test WITHOUT Auron
        Configuration configNoAuron = new Configuration();
        configNoAuron.setBoolean("table.optimizer.auron.enabled", false);
        configNoAuron.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        StreamExecutionEnvironment envNoAuron =
            StreamExecutionEnvironment.getExecutionEnvironment(configNoAuron);
        StreamTableEnvironment tEnvNoAuron = StreamTableEnvironment.create(envNoAuron);

        tEnvNoAuron.executeSql(
            "CREATE TABLE sales_no_auron (" +
            "  id BIGINT, product STRING, amount DOUBLE" +
            ") WITH ('connector' = 'filesystem', 'path' = 'file:///tmp/sales.parquet', 'format' = 'parquet')"
        );

        String planWithoutAuron = tEnvNoAuron.explainSql(
            "SELECT id FROM sales_no_auron WHERE amount > 100"
        );

        // Test WITH Auron
        Configuration configWithAuron = new Configuration();
        configWithAuron.setBoolean("table.optimizer.auron.enabled", true);
        configWithAuron.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        StreamExecutionEnvironment envWithAuron =
            StreamExecutionEnvironment.getExecutionEnvironment(configWithAuron);
        StreamTableEnvironment tEnvWithAuron = StreamTableEnvironment.create(envWithAuron);

        tEnvWithAuron.executeSql(
            "CREATE TABLE sales_with_auron (" +
            "  id BIGINT, product STRING, amount DOUBLE" +
            ") WITH ('connector' = 'filesystem', 'path' = 'file:///tmp/sales.parquet', 'format' = 'parquet')"
        );

        String planWithAuron = tEnvWithAuron.explainSql(
            "SELECT id FROM sales_with_auron WHERE amount > 100"
        );

        System.out.println("\n==================== WITHOUT AURON ====================");
        System.out.println(planWithoutAuron);
        System.out.println("\n==================== WITH AURON ====================");
        System.out.println(planWithAuron);

        // Verify they're different
        assertNotEquals(planWithoutAuron, planWithAuron,
            "Plans should differ when Auron is enabled vs disabled");

        // Verify Auron appears in the enabled plan but not the disabled plan
        assertFalse(planWithoutAuron.contains("Auron"),
            "Plan without Auron should not contain 'Auron'");
        assertTrue(planWithAuron.contains("Auron"),
            "Plan with Auron should contain 'Auron'");
    }
}
```

## Method 4: Check Runtime Execution

Monitor the actual execution to see Auron's native engine running.

```java
@Test
public void testAuronNativeExecutionRuns() throws Exception {
    // Enable Auron
    Configuration config = new Configuration();
    config.setBoolean("table.optimizer.auron.enabled", true);
    config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

    // Enable verbose logging
    config.setString("table.exec.auron.log-level", "DEBUG");

    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(config);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // Create table and execute query
    tEnv.executeSql(
        "CREATE TABLE test_data (" +
        "  id BIGINT, name STRING, value DOUBLE" +
        ") WITH ('connector' = 'filesystem', 'path' = 'file:///tmp/data.parquet', 'format' = 'parquet')"
    );

    // Execute and collect results
    TableResult result = tEnv.executeSql(
        "SELECT id, name FROM test_data WHERE value > 50"
    );

    // If this runs without error and you see Auron logs, it's working!
    result.print();
}
```

### Expected Runtime Logs

When Auron executes, you'll see:
```
INFO  FlinkAuronAdaptor - Initializing Auron native execution
INFO  FlinkAuronAdaptor - Creating native plan with 1 partitions
[INFO] [auron::exec] - entering native environment
[INFO] [auron::exec] - executing physical plan: PROJECTION -> FILTER -> PARQUET_SCAN
[INFO] [auron::exec] - ParquetScan: reading from file:///tmp/data.parquet
[INFO] [auron::exec] - Filter: applying predicate value > 50
[INFO] [auron::exec] - Projection: selecting [id, name]
[INFO] [auron_memmgr] - mem manager status: mem_used: 2.5 MB
[INFO] [auron::exec] - exiting native environment
```

## Method 5: Use Flink Web UI

If running Flink cluster with Web UI:

1. Start Flink cluster: `./bin/start-cluster.sh`
2. Submit job with Auron enabled
3. Go to `http://localhost:8081`
4. Click on your job
5. View the execution plan diagram

**Expected:** You should see a node labeled `Auron Native Execution` or `AuronBatchExecNode` in the visual plan.

## Method 6: Intercept ExecNode Graph

For deep verification, you can intercept the ExecNode graph:

```java
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import java.lang.reflect.Field;

public class ExecNodeGraphInspector {

    public static void inspectExecNodeGraph(StreamTableEnvironment tEnv, String sql) {
        try {
            // Parse the SQL
            tEnv.sqlQuery(sql);

            // Access the planner (requires reflection)
            Field plannerField = tEnv.getClass().getDeclaredField("planner");
            plannerField.setAccessible(true);
            PlannerBase planner = (PlannerBase) plannerField.get(tEnv);

            // This is complex - you'd need to call internal methods
            // Easier to just use EXPLAIN and logs

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

## Quick Verification Checklist

✅ **Checklist:**

1. **Configuration is correct:**
   ```java
   assertTrue(config.getBoolean("table.optimizer.auron.enabled", false));
   ```

2. **Auron JAR is on classpath:**
   ```bash
   ls -lh $FLINK_HOME/lib/auron-flink-*.jar
   # Should show the Auron JAR
   ```

3. **Native library is available:**
   ```java
   try {
       System.loadLibrary("auron");
       System.out.println("✅ Auron native library loaded");
   } catch (UnsatisfiedLinkError e) {
       System.out.println("❌ Auron native library NOT available");
   }
   ```

4. **Flink version compatibility:**
   ```bash
   # Check Flink version
   cat $FLINK_HOME/META-INF/MANIFEST.MF | grep Implementation-Version

   # Should be compatible with auron-flink-extension
   ```

5. **Log level is appropriate:**
   ```properties
   logger.auron.level = INFO  # or DEBUG for more detail
   ```

## Troubleshooting

### Issue: No Auron in execution plan

**Possible causes:**
1. `table.optimizer.auron.enabled` is false
2. Auron JAR not on classpath
3. Query pattern not supported (not Parquet scan)
4. Processor failed to load

**Debug:**
```bash
# Check if class exists
java -cp $FLINK_HOME/lib/'*' \
  org.apache.flink.table.planner.plan.nodes.exec.processor.AuronExecNodeGraphProcessor

# Should print class info or "Main method not found" (which is OK)
```

### Issue: "Auron not available on classpath" warning

**Solution:**
```bash
# Add Auron JAR to Flink lib directory
cp /path/to/auron-flink-planner-7.0.0-SNAPSHOT.jar $FLINK_HOME/lib/

# Restart Flink cluster
$FLINK_HOME/bin/stop-cluster.sh
$FLINK_HOME/bin/start-cluster.sh
```

### Issue: Native library not found

**Solution:**
```bash
# Check if libauron.so/dylib is accessible
export LD_LIBRARY_PATH=/path/to/native/libs:$LD_LIBRARY_PATH  # Linux
export DYLD_LIBRARY_PATH=/path/to/native/libs:$DYLD_LIBRARY_PATH  # Mac

# Or add to Flink config
export FLINK_ENV_JAVA_OPTS="-Djava.library.path=/path/to/native/libs"
```

## Summary

**Best verification methods in order:**

1. **EXPLAIN command** - Quick, easy, conclusive
2. **Check logs** - Shows conversion happening
3. **Programmatic test** - Automated verification
4. **Runtime logs** - Confirms native execution
5. **Web UI** - Visual confirmation

The simplest way is:
```java
String plan = tEnv.explainSql("SELECT ... FROM parquet_table WHERE ...");
if (plan.contains("Auron")) {
    System.out.println("✅ Using Auron native execution!");
} else {
    System.out.println("❌ NOT using Auron");
}
```
