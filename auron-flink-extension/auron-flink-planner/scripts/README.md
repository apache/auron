# Auron-Flink Testing Scripts

This directory contains essential scripts for testing and benchmarking the Auron-Flink integration.

## Core Scripts

### 1. generate-data-on-cluster.sh
Generates Parquet test data using Flink cluster parallelism.

**Usage:**
```bash
./generate-data-on-cluster.sh <num_rows> [wide]
```

**Examples:**
```bash
# Generate 10M rows with standard schema (4 columns)
./generate-data-on-cluster.sh 10000000

# Generate 5M rows with wide schema (50 columns)
./generate-data-on-cluster.sh 5000000 wide
```

**Standard Schema (4 columns):**
- id: BIGINT
- product: STRING
- amount: DOUBLE
- category: STRING

**Wide Schema (50 columns):**
- 10 id columns (id0-id9): BIGINT
- 20 metric columns (metric0-metric19): DOUBLE
- 20 dimension columns (dim0-dim19): STRING

**Output:**
Data is written to `/tmp/flink_auron_<rows>_<timestamp>` or `/tmp/flink_auron_wide_<rows>_<timestamp>`

### 2. run-query.sh
Flexible query runner that executes arbitrary SQL with Auron and/or Flink native.

**Usage:**
```bash
./run-query.sh <data_path> <table_schema> <sql_query> [mode]
```

**Parameters:**
- `data_path`: Path to Parquet data directory
- `table_schema`: Column definitions (e.g., 'id BIGINT, name STRING, amount DOUBLE')
- `sql_query`: SQL query to execute
- `mode`: 'both' (default), 'auron', or 'native'

**Examples:**
```bash
# Standard table - run with both Auron and Flink native
./run-query.sh /tmp/flink_auron_10000000_1769658711 \
  'id BIGINT, product STRING, amount DOUBLE, category STRING' \
  'SELECT id, amount FROM sales WHERE amount > 1000 LIMIT 100'

# Wide table - column pruning test
SCHEMA='id0 BIGINT, id1 BIGINT, ..., dim19 STRING'
./run-query.sh /tmp/flink_auron_wide_5000000_1769659081 \
  "$SCHEMA" \
  'SELECT id0, metric0, metric5, dim0 FROM sales WHERE metric0 > 5000 LIMIT 100'

# Run only with Auron
./run-query.sh /tmp/data 'id BIGINT, name STRING' 'SELECT * FROM sales' auron
```

### 3. compare-query.sh
Validates correctness by comparing query results between Auron and Flink native.

**Usage:**
```bash
./compare-query.sh <data_path> <table_schema> <sql_query>
```

**What it validates:**
1. Row counts match
2. All row contents are identical
3. Performance comparison

**Example:**
```bash
./compare-query.sh /tmp/flink_auron_10000000_1769658711 \
  'id BIGINT, product STRING, amount DOUBLE, category STRING' \
  'SELECT id, amount FROM sales WHERE amount > 1000 LIMIT 100'
```

**Output:**
- ✅ Success: Exits with 0 if results match
- ❌ Failure: Exits with 1 if row counts or contents differ

### 4. run-e2e-test.sh
Runs end-to-end verification tests (AuronExecutionVerificationTest).

**Usage:**
```bash
./run-e2e-test.sh
```

This test:
- Creates temporary Parquet test data
- Executes queries with Auron enabled/disabled
- Verifies Auron conversion logs
- Displays actual query results

## Prerequisites

### Flink Cluster
All scripts require a running Flink cluster:
```bash
/path/to/flink/bin/start-cluster.sh
```

Verify cluster is running:
```bash
curl http://localhost:8081/overview
```

### Java Version
Set JAVA_HOME to Java 17:
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
```

### Flink Configuration
Ensure `/path/to/flink/conf/flink-conf.yaml` has:
```yaml
parallelism.default: 4
execution.runtime-mode: BATCH
table.optimizer.auron.enabled: true
env.java.home: /Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
taskmanager.numberOfTaskSlots: 4
taskmanager.memory.process.size: 8g
```

### Dependencies in Flink lib/
The following JARs must be in Flink's lib directory:
- `auron-flink-assembly-7.0.0-SNAPSHOT.jar`
- Parquet 1.13.1 JARs (column, common, encoding, format-structures, hadoop, jackson)
- Hadoop 3.3.4 JARs (auth, common, hdfs-client, mapreduce-client-core)

## Typical Workflow

### 1. Generate Test Data
```bash
# Generate 10M rows
./generate-data-on-cluster.sh 10000000

# Note the output path, e.g., /tmp/flink_auron_10000000_1769658711
```

### 2. Run Queries
```bash
# Quick test with both modes
./run-query.sh /tmp/flink_auron_10000000_1769658711 \
  'id BIGINT, product STRING, amount DOUBLE, category STRING' \
  'SELECT * FROM sales WHERE amount > 1000 LIMIT 100'
```

### 3. Validate Correctness
```bash
# Ensure results match
./compare-query.sh /tmp/flink_auron_10000000_1769658711 \
  'id BIGINT, product STRING, amount DOUBLE, category STRING' \
  'SELECT * FROM sales WHERE amount > 1000 LIMIT 100'
```

### 4. Benchmark Performance
```bash
# Test various query patterns
./run-query.sh /tmp/data 'schema' 'SELECT * FROM sales LIMIT 1000000' both
./run-query.sh /tmp/data_wide 'wide_schema' 'SELECT col1, col2 FROM sales WHERE ...' both
```

## Performance Notes

Current observations:
- **LIMIT queries**: Flink native is typically faster (~1.9x) due to JNI overhead and early termination
- **Large result sets**: Auron's vectorization should excel (pending benchmarks)
- **Column pruning**: Auron's native Parquet reader can skip unused columns efficiently
- **Complex filters**: SIMD operations should accelerate bulk filtering

See Task #8 for benchmark query construction.

## Troubleshooting

### "Flink cluster is not running"
Start the cluster:
```bash
/path/to/flink/bin/start-cluster.sh
```

### "ClassNotFoundException"
Ensure `auron-flink-assembly-7.0.0-SNAPSHOT.jar` is in Flink's lib directory and restart the cluster.

### "UnsupportedClassVersionError"
Set `env.java.home` in flink-conf.yaml to Java 17 and restart the cluster.

### Compilation errors
If scripts need to recompile Java files, ensure you've built Auron:
```bash
cd /Users/vsowrira/git/auron
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
./build/apache-maven-3.9.12/bin/mvn clean install -DskipTests \
  -Pflink-1.18 -Pspark-3.5 -Pscala-2.12
```

## Archived Scripts

Obsolete scripts have been moved to `../archive/obsolete-scripts/`:
- `generate-100k-data.sh`, `generate-large-testdata.sh`, `generate-wide-testdata.sh` - replaced by `generate-data-on-cluster.sh`
- `query-on-cluster.sh`, `test-wide-table.sh` - replaced by `run-query.sh`
- `monitor-and-test.sh` - hardcoded job IDs, not reusable
- Various test runners - consolidated into `run-e2e-test.sh`

Obsolete Java files in `../archive/obsolete-java/`:
- `QueryOnCluster.java`, `QueryOnClusterWide.java` - replaced by `FlexibleQueryRunner.java`
