# Flink-Auron Batch Integration Guide

## Overview

This document describes the automatic integration between Apache Flink batch execution and Auron native engine. When enabled, Flink SQL queries that use supported operators (Parquet scans, filters, projections) will automatically execute using Auron's native vectorized engine for improved performance.

## Architecture

### Integration Points

The integration consists of components in both Flink and Auron repositories:

**Flink Components** (`/Users/vsowrira/git/flink`):

1. **Configuration Option** (`flink-table-api-java/src/main/java/org/apache/flink/table/api/config/OptimizerConfigOptions.java`)
   - `TABLE_OPTIMIZER_AURON_ENABLED` - Master switch for Auron execution

2. **ExecNode Graph Processor** (`flink-table-planner/src/main/java/org/apache/flink/table/planner/plan/nodes/exec/processor/AuronExecNodeGraphProcessor.java`)
   - Traverses the ExecNode graph after physical planning
   - Identifies supported patterns (Parquet scan + calc)
   - Replaces compatible nodes with AuronBatchExecNode

3. **Auron ExecNode Wrapper** (`flink-table-planner/src/main/java/org/apache/flink/table/planner/plan/nodes/exec/batch/AuronBatchExecNode.java`)
   - Wraps Flink ExecNodes for Auron execution
   - Delegates to Auron converter and transformation factory
   - Uses reflection to avoid hard compile-time dependency

4. **BatchPlanner Registration** (`flink-table-planner/src/main/scala/org/apache/flink/table/planner/delegation/BatchPlanner.scala`)
   - Registers AuronExecNodeGraphProcessor when enabled
   - Runs FIRST in the processor chain to intercept before other optimizations

**Auron Components** (`/Users/vsowrira/git/auron/auron-flink-extension`):

5. **ExecNode Converter** (`auron-flink-planner/src/main/java/org/apache/auron/flink/planner/AuronExecNodeConverter.java`)
   - Extracts information from Flink ExecNodes
   - Converts to Auron PhysicalPlanNode protobuf
   - Handles scan-only and scan+calc patterns

6. **Transformation Factory** (`auron-flink-planner/src/main/java/org/apache/auron/flink/planner/AuronTransformationFactory.java`)
   - Creates Flink transformations from Auron plans
   - Uses AuronBatchExecutionWrapperOperator
   - Configures parallelism and metadata

### Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. User submits SQL query                                       │
│    SELECT id, name FROM parquet_table WHERE amount > 100        │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Flink Calcite Planner                                        │
│    - Parse SQL → AST                                            │
│    - Logical optimization                                       │
│    - Physical planning → FlinkPhysicalRel tree                  │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. ExecNodeGraphGenerator                                       │
│    - Convert FlinkPhysicalRel → ExecNode tree                   │
│    - Creates: BatchExecCalc → BatchExecTableSourceScan          │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. AuronExecNodeGraphProcessor (NEW)                            │
│    - Detects: Calc + Parquet Scan pattern                      │
│    - Checks: TABLE_OPTIMIZER_AURON_ENABLED = true              │
│    - Checks: Auron classes available on classpath              │
│    - Replaces: With AuronBatchExecNode wrapper                 │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. Other ExecNode Processors                                    │
│    - DeadlockBreakupProcessor, DynamicFilteringProcessor, etc. │
│    - See AuronBatchExecNode as a normal node                   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 6. Plan Translation: AuronBatchExecNode.translateToPlan()       │
│    - Calls AuronExecNodeConverter.convert()                    │
│      → Extracts file paths, schema, filters, projections       │
│      → Builds PhysicalPlanNode protobuf                         │
│    - Calls AuronTransformationFactory.createTransformation()   │
│      → Creates Flink transformation with Auron operator         │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 7. Flink Runtime Execution                                      │
│    - AuronBatchExecutionWrapperOperator.processElement()       │
│    - FlinkAuronAdaptor → JNI → Rust native engine             │
│    - DataFusion executes: ParquetExec → FilterExec →           │
│                           ProjectionExec                        │
│    - Results returned via Arrow FFI → Flink RowData            │
└─────────────────────────────────────────────────────────────────┘
```

## Supported Patterns

### Pattern 1: Parquet Scan Only

**Flink SQL:**
```sql
SELECT * FROM parquet_table
```

**ExecNode Graph:**
```
BatchExecTableSourceScan(table=[parquet_table], format=[Parquet])
```

**Converted to:**
```
AuronBatchExecNode[
  PhysicalPlanNode { parquet_scan { ... } }
]
```

### Pattern 2: Scan + Filter

**Flink SQL:**
```sql
SELECT * FROM parquet_table WHERE amount > 100
```

**ExecNode Graph:**
```
BatchExecCalc(condition=[amount > 100])
  └─ BatchExecTableSourceScan(table=[parquet_table])
```

**Converted to:**
```
AuronBatchExecNode[
  PhysicalPlanNode {
    parquet_scan {
      pruning_predicates=[amount > 100]
    }
  }
]
```

### Pattern 3: Scan + Projection

**Flink SQL:**
```sql
SELECT id, name FROM parquet_table
```

**ExecNode Graph:**
```
BatchExecCalc(projection=[id, name])
  └─ BatchExecTableSourceScan(table=[parquet_table])
```

**Converted to:**
```
AuronBatchExecNode[
  PhysicalPlanNode {
    projection {
      input { parquet_scan { ... } }
      expr=[id, name]
    }
  }
]
```

### Pattern 4: Scan + Filter + Projection

**Flink SQL:**
```sql
SELECT id, name FROM parquet_table WHERE amount > 100
```

**ExecNode Graph:**
```
BatchExecCalc(projection=[id, name], condition=[amount > 100])
  └─ BatchExecTableSourceScan(table=[parquet_table])
```

**Converted to:**
```
AuronBatchExecNode[
  PhysicalPlanNode {
    projection {
      input {
        parquet_scan {
          pruning_predicates=[amount > 100]
        }
      }
      expr=[id, name]
    }
  }
]
```

## Configuration

### Enabling Auron

Add to your Flink configuration (`conf/flink-conf.yaml` or programmatically):

```yaml
# Enable Auron native execution
table.optimizer.auron.enabled: true

# Set runtime mode to BATCH (required)
execution.runtime-mode: BATCH
```

Or programmatically:

```java
Configuration config = new Configuration();
config.setBoolean("table.optimizer.auron.enabled", true);
config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
```

### Additional Auron Configuration

From the Auron extension library (already supported):

```yaml
# Batch size for vectorized operations
table.exec.auron.batch-size: 8192

# Memory fraction for native operations
table.exec.auron.memory-fraction: 0.7

# Logging level for native engine
table.exec.auron.log-level: INFO
```

## Building and Deployment

### Build Flink with Auron Support

```bash
cd /Users/vsowrira/git/flink
mvn clean install -DskipTests -Pfast
```

### Build Auron Extension

```bash
cd /Users/vsowrira/git/auron
./auron-build.sh
```

### Deploy

1. Copy Auron JAR to Flink lib directory:
   ```bash
   cp /Users/vsowrira/git/auron/target/auron-flink-*.jar \
      /path/to/flink/lib/
   ```

2. Ensure native library is available:
   ```bash
   # The JAR should include libauron.so/dylib in native resources
   # Or set java.library.path:
   export FLINK_ENV_JAVA_OPTS="-Djava.library.path=/path/to/native/libs"
   ```

3. Start Flink cluster:
   ```bash
   /path/to/flink/bin/start-cluster.sh
   ```

## Testing

### Example Test Query

```java
import org.apache.flink.table.api.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.*;

public class AuronIntegrationTest {
    public static void main(String[] args) throws Exception {
        // Setup environment with Auron enabled
        Configuration config = new Configuration();
        config.setBoolean("table.optimizer.auron.enabled", true);
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Create Parquet table
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

        // Execute query - should use Auron automatically
        TableResult result = tEnv.executeSql(
            "SELECT id, product, amount " +
            "FROM sales " +
            "WHERE amount > 100 " +
            "ORDER BY amount DESC"
        );

        result.print();
    }
}
```

### Verify Auron Execution

Check the Flink job logs for:

```
INFO  AuronExecNodeGraphProcessor - Auron native execution is available and will be used for supported operators
INFO  AuronExecNodeGraphProcessor - Converted BatchExecCalc to Auron native execution: ...
INFO  AuronTransformationFactory - Creating Auron transformation for plan type: PARQUET_SCAN
INFO  FlinkAuronAdaptor - Initializing Auron native execution
```

### Use EXPLAIN to See Plan

```sql
EXPLAIN SELECT id, product FROM sales WHERE amount > 100;
```

Should show `AuronBatchExecNode` in the execution plan.

## Troubleshooting

### Auron Not Being Used

**Check 1: Configuration**
```java
// Verify Auron is enabled
boolean enabled = tEnv.getConfig().getConfiguration()
    .getBoolean("table.optimizer.auron.enabled", false);
System.out.println("Auron enabled: " + enabled);
```

**Check 2: Classpath**
```bash
# Verify Auron JAR is in classpath
ls -l $FLINK_HOME/lib/auron-flink-*.jar

# Check if classes are available
java -cp $FLINK_HOME/lib/'*' \
  -XshowSettings:class \
  -version 2>&1 | grep -i auron
```

**Check 3: Native Library**
```bash
# Check if native library loads
java -Djava.library.path=/path/to/libs \
  -cp $FLINK_HOME/lib/'*' \
  -c 'System.loadLibrary("auron"); System.out.println("Loaded")'
```

**Check 4: Logs**
```bash
# Check Flink TaskManager logs for warnings
grep -i auron $FLINK_HOME/log/flink-*-taskexecutor-*.log
```

### Common Issues

1. **"Auron not available on classpath"**
   - Solution: Add auron-flink-extension JAR to Flink lib directory

2. **"UnsatisfiedLinkError: no auron in java.library.path"**
   - Solution: Ensure libauron.so/dylib is in library path or embedded in JAR

3. **"Unsupported ExecNode pattern"**
   - Solution: Check that your query uses Parquet tables and supported operations

4. **"Failed to extract file paths"**
   - Solution: Ensure table has 'path' option in WITH clause

## Future Enhancements

### Planned Features

1. **Additional Operators**
   - Hash joins
   - Aggregations
   - Window functions
   - Sorts

2. **Improved Format Detection**
   - Auto-detect Parquet vs ORC vs other formats
   - Support for more file formats

3. **Better Error Handling**
   - Graceful fallback to Flink execution on errors
   - More detailed error messages

4. **Performance Tuning**
   - Auto-tuning of parallelism based on data size
   - Dynamic memory management
   - Adaptive execution strategies

5. **Monitoring**
   - Auron-specific metrics in Flink UI
   - Performance comparison reports
   - Query profiling

## Implementation Details

### Why ExecNodeGraphProcessor?

We chose `ExecNodeGraphProcessor` as the integration point because:

1. **Official Extension Mechanism**: It's Flink's intended way to transform execution graphs
2. **Post-Optimization**: Runs after Calcite optimization, so we get optimized plans
3. **Pre-Translation**: Runs before translation to transformations, allowing us to substitute entire subgraphs
4. **Configurable**: Can be enabled/disabled via configuration
5. **Isolated**: Minimal impact on Flink core when disabled

### Reflection-Based Integration

The integration uses reflection to avoid hard compile-time dependencies:

- **Flink → Auron**: AuronBatchExecNode uses reflection to call Auron converter
- **Auron Availability Check**: Uses Class.forName() to check if Auron is on classpath
- **Graceful Degradation**: If Auron is not available, falls back to standard Flink execution

This design allows:
- Flink to work without Auron present
- Auron to be optionally added at deployment time
- No circular dependencies between projects

## Contact

For issues or questions about the Flink-Auron integration:
- File issues in the Auron repository
- Check Auron documentation
- Contact the development team

## References

- [Auron Architecture Document](ARCHITECTURE.md)
- [Flink Table API Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/overview/)
- [ExecNode Graph Processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/concepts/planner/)
