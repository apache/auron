# Quick Start Guide - Flink with Auron

This guide provides simplified commands for building and testing Auron with Flink support.

## Prerequisites

- Java 17: `/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home`
- Rust nightly toolchain (for native library)
- Maven 3.9.12 (bundled in `build/` directory)
- Flink 1.18 with Auron integration: `/Users/vsowrira/git/flink` (branch: `auron-flink-1.18-integration`)

## Build

### Unified Build (Recommended)

Build both Flink and Auron with a single command:

```bash
# Build everything (first time: ~15-20 minutes)
./build-all.sh

# Fast build (skip tests, ~5-8 minutes)
./build-all.sh --skip-tests

# Build only Auron (assumes Flink already built)
./build-all.sh --auron-only

# Build only Flink
./build-all.sh --flink-only

# Clean rebuild from scratch
./build-all.sh --clean
```

**What this does:**
1. Checks if Flink needs to be built (smart detection)
2. Builds Flink 1.18-SNAPSHOT with Auron integration (if needed)
3. Builds Auron against those Flink JARs
4. Verifies integration classes are present

**First build**: 15-20 minutes (Flink: 10-15 min, Auron: 3-5 min)
**Subsequent builds**: 3-5 minutes (Flink build is skipped if up-to-date)

### Auron-Only Build

If Flink is already built and you only need to rebuild Auron:

```bash
./build-flink.sh          # Build and install (skip tests) - DEFAULT
./build-flink.sh clean    # Clean build from scratch
./build-flink.sh test     # Build and run tests
```

### Manual Build

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
./build/apache-maven-3.9.12/bin/mvn clean install -DskipTests -Pflink-1.18 -Pscala-2.12
```

For comprehensive build documentation, see [BUILD-GUIDE.md](BUILD-GUIDE.md).

## Run Examples

### Simplified Runner (Recommended)

```bash
cd auron-flink-extension/auron-flink-planner

# Run GROUP BY hybrid execution demo (default)
./run-example.sh groupby

# Run parallel test with 50K rows, parallelism=4
./run-example.sh parallel

# Run MVP example with parallelism=1
./run-example.sh mvp
```

### What Each Example Demonstrates

#### `groupby` - Hybrid Execution Demo
- **Dataset**: 10,000 rows across 10 categories
- **Parallelism**: 4
- **Demonstrates**:
  - Auron native ParquetScan (reads Parquet with columnar processing)
  - Flink GROUP BY aggregations (COUNT, SUM, AVG, MIN, MAX)
  - Seamless data handoff between Auron (native) and Flink (JVM)
- **Key Insight**: Shows how Auron accelerates I/O while Flink handles complex logic

#### `parallel` - Distributed Execution
- **Dataset**: 50,000 rows
- **Parallelism**: 4
- **Demonstrates**:
  - File splitting across parallel tasks
  - Distributed Parquet scanning with 4 concurrent tasks
  - Large-scale data processing (50K rows counted in ~1.6 seconds)
- **Key Insight**: Validates Phase 1 implementation with higher parallelism and larger datasets

#### `mvp` - Basic Integration
- **Dataset**: 100 rows
- **Parallelism**: 1
- **Demonstrates**:
  - Basic Auron configuration
  - Full scan, projection, filter, count queries
  - Clean integration with Flink Table API
- **Key Insight**: Simple working example for getting started

## Integration Tests

For comprehensive integration tests:

```bash
cd auron-flink-extension/auron-flink-planner

# Run execution verification test (shows Auron conversion logs)
./run-e2e-test-final.sh

# Run specific test types
./run-e2e-test-final.sh Execution  # Shows Auron native execution logs
./run-e2e-test-final.sh Simple     # Basic infrastructure test
./run-e2e-test-final.sh Manual     # Manual end-to-end test
```

## Direct Execution (Advanced)

If you need full control over Java execution:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

CLASSPATH="auron-flink-extension/auron-flink-planner/target/test-classes:\
auron-flink-extension/auron-flink-planner/target/classes:\
auron-flink-extension/auron-flink-planner/target/lib/*"

$JAVA_HOME/bin/java --add-opens=java.base/java.nio=ALL-UNNAMED \
  org.apache.auron.flink.examples.AuronFlinkGroupByTest
```

Available test classes:
- `org.apache.auron.flink.examples.AuronFlinkGroupByTest`
- `org.apache.auron.flink.examples.AuronFlinkParallelTest`
- `org.apache.auron.flink.examples.AuronFlinkMVPWorkingExample`

## Expected Output

When tests run successfully, you should see:

```
------ initializing auron native environment ------
[INFO] [auron::exec:70] - initializing JNI bridge
[INFO] [auron_jni_bridge::jni_bridge:491] - ==> FLINK MODE: Spark/Scala classes will be skipped
[INFO] [auron::rt:147] - start executing plan:
ParquetExec: limit=None, file_group=[...]
[INFO] [datafusion_datasource_parquet::opener:421] - executing parquet scan with adaptive batch size: 8192
[INFO] [auron::rt:188] - task finished
✅ All queries executed successfully with Auron!
```

## Troubleshooting

**"Cannot find libauron.dylib"**: Run `./build-flink.sh` to build the native library.

**"ClassNotFoundException"**: Dependencies need to be copied. The `run-example.sh` script handles this automatically.

**Architecture mismatch**: Ensure you're using ARM64 Java on Apple Silicon (or x86_64 on Intel).

**Arrow memory error**: The `--add-opens=java.base/java.nio=ALL-UNNAMED` JVM flag is required for Java 17+.

## Performance Notes

- **First run**: ~30 seconds (copies dependencies, compiles if needed)
- **Subsequent runs**: Immediate (uses cached dependencies)
- **Native library build**: ~3 minutes (only when code changes)

## Next Steps

After running examples:
1. Review logs to see Auron native execution
2. Modify query parameters in test classes
3. Add your own test data and queries
4. Monitor memory usage via Auron's memory manager logs

For more details, see:
- [CLAUDE.md](CLAUDE.md) - Development guidelines
- [README.md](README.md) - Full documentation
- [auron-flink-extension/auron-flink-planner/](auron-flink-extension/auron-flink-planner/) - Test source code
