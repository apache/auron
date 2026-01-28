# Complete Build Guide - Flink + Auron Integration

This guide explains how to build both Flink (with Auron integration) and Auron itself using the simplified unified build process.

## Overview

The Auron-Flink integration consists of two components:

1. **Flink 1.18 with Auron Integration** (`/Users/vsowrira/git/flink`)
   - Branch: `auron-flink-1.18-integration`
   - Adds automatic conversion of batch queries to Auron native execution
   - Produces `flink-table-planner_2.12-1.18-SNAPSHOT.jar` with Auron classes

2. **Auron with Flink Support** (`/Users/vsowrira/git/auron`)
   - Module: `auron-flink-extension/auron-flink-planner`
   - Implements converters and transformation factories
   - Depends on Flink 1.18-SNAPSHOT JARs

## Prerequisites

- **Java 17**: `/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home`
  - Required for Maven plugins and Apache Arrow
  - Project compiles to Java 8 bytecode for compatibility

- **Rust nightly**: For building Auron native library

- **Maven 3.9.12**: Bundled in `build/` directory (Auron) and via Maven wrapper (Flink)

## Quick Start - Unified Build

The simplest way to build everything:

```bash
cd /Users/vsowrira/git/auron

# Build both Flink and Auron (first time)
./build-all.sh

# Or faster build (skip tests)
./build-all.sh --skip-tests
```

This single command:
1. ✅ Checks if Flink needs to be built
2. ✅ Builds Flink 1.18-SNAPSHOT with Auron integration (if needed)
3. ✅ Installs Flink JARs to Maven local repository
4. ✅ Builds Auron against those Flink JARs
5. ✅ Verifies integration classes are present

**First build**: Takes ~15-20 minutes (Flink: 10-15 min, Auron: 3-5 min)
**Subsequent builds**: Much faster, Flink build is skipped if up-to-date

## Build Options

### Build Only Flink

If you've made changes to Flink integration code:

```bash
./build-all.sh --flink-only
```

This builds only Flink and installs to Maven local.

### Build Only Auron

If Flink is already built and you only need to rebuild Auron:

```bash
./build-all.sh --auron-only
```

This assumes Flink 1.18-SNAPSHOT JARs are already in Maven local.

### Clean Builds

Force a clean build from scratch:

```bash
# Clean both
./build-all.sh --clean

# Clean only Flink
./build-all.sh --flink-clean

# Clean only Auron
./build-all.sh --auron-clean
```

### Fast Build (Skip Tests)

For iterative development:

```bash
./build-all.sh --skip-tests
```

## Manual Build Process

If you prefer to build components separately:

### Step 1: Build Flink 1.18 with Auron Integration

```bash
cd /Users/vsowrira/git/flink

# Verify branch
git branch --show-current  # Should be: auron-flink-1.18-integration

# Build only required modules (faster)
./mvnw clean install -DskipTests \
  -pl flink-table/flink-table-api-java,flink-table/flink-table-planner \
  -am \
  -Pscala-2.12
```

**What this builds:**
- `flink-table-api-java-1.18-SNAPSHOT.jar` - Contains `OptimizerConfigOptions`
- `flink-table-planner_2.12-1.18-SNAPSHOT.jar` - Contains `AuronExecNodeGraphProcessor` and `AuronBatchExecNode`

**Build time**: 10-15 minutes (first time), 2-3 minutes (incremental)

**Verification:**
```bash
# Check Auron classes are in the JAR
jar tf ~/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/flink-table-planner_2.12-1.18-SNAPSHOT.jar | grep Auron

# Should show:
# org/apache/flink/table/planner/plan/nodes/exec/processor/AuronExecNodeGraphProcessor.class
# org/apache/flink/table/planner/plan/nodes/exec/batch/AuronBatchExecNode.class
```

### Step 2: Build Auron with Flink Support

```bash
cd /Users/vsowrira/git/auron

# Quick build (uses build-flink.sh wrapper)
./build-flink.sh

# Or full Maven command
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
./build/apache-maven-3.9.12/bin/mvn clean install -DskipTests \
  -Pflink-1.18 -Pscala-2.12
```

**What this builds:**
- Native library: `native-engine/src-flink/.../libauron.dylib` (Rust)
- Java modules: `auron-flink-planner-7.0.0-SNAPSHOT.jar`
- Test classes for integration verification

**Build time**: 3-5 minutes (native lib compile is the slowest part)

## Verify the Build

### Check Flink Integration

```bash
cd /Users/vsowrira/git/flink

# Verify Auron integration classes
jar tf ~/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/flink-table-planner_2.12-1.18-SNAPSHOT.jar | grep Auron
```

Expected output:
```
org/apache/flink/table/planner/plan/nodes/exec/processor/AuronExecNodeGraphProcessor.class
org/apache/flink/table/planner/plan/nodes/exec/batch/AuronBatchExecNode.class
```

### Check Auron Converter Classes

```bash
cd /Users/vsowrira/git/auron

jar tf auron-flink-extension/auron-flink-planner/target/auron-flink-planner-7.0.0-SNAPSHOT.jar | grep -E "(Converter|Transformation)"
```

Expected classes:
- `AuronExecNodeConverter.class`
- `AuronTransformationFactory.class`
- `AuronBatchExecutionWrapperOperator.class`

### Check Dependency Version

Verify Auron is using the correct Flink version:

```bash
cd /Users/vsowrira/git/auron
./build/apache-maven-3.9.12/bin/mvn dependency:tree \
  -pl auron-flink-extension/auron-flink-planner \
  -Pflink-1.18 -Pscala-2.12 | grep flink-table-planner
```

Should show: `org.apache.flink:flink-table-planner_2.12:jar:1.18-SNAPSHOT:compile`

## Run Examples

After building, test the integration:

```bash
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner

# GROUP BY hybrid execution demo (recommended)
./run-example.sh groupby

# Parallel execution with 50K rows
./run-example.sh parallel

# Basic MVP example
./run-example.sh mvp
```

**Expected output:**
```
------ initializing auron native environment ------
[INFO] [auron::exec:70] - initializing JNI bridge
[INFO] [auron_jni_bridge::jni_bridge:491] - ==> FLINK MODE: Spark/Scala classes will be skipped
[INFO] [auron::rt:147] - start executing plan:
ParquetExec: limit=None, file_group=[...]
[INFO] [datafusion_datasource_parquet::opener:421] - executing parquet scan with adaptive batch size: 8192
[INFO] [auron::rt:188] - task finished
✅ Query completed successfully with Auron!
```

## Run Integration Tests

Comprehensive integration tests:

```bash
cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner

# Run execution verification test (shows Auron conversion logs)
./run-e2e-test-final.sh

# Or specific test types
./run-e2e-test-final.sh Execution  # AuronExecutionVerificationTest (default)
./run-e2e-test-final.sh Simple     # AuronSimpleVerificationTest
./run-e2e-test-final.sh Manual     # AuronEndToEndManualTest
```

Look for this log message to confirm conversion is working:
```
INFO  o.a.f.t.p.p.n.e.p.AuronExecNodeGraphProcessor - Converted BatchPhysicalTableSourceScan to Auron native execution
```

## Troubleshooting

### Flink Build Fails

**Error**: Module not found or dependency resolution error

**Solution**: Build from Flink root with `-am` flag:
```bash
cd /Users/vsowrira/git/flink
./mvnw clean install -DskipTests -am -Pscala-2.12
```

### Auron Build Fails - Missing Flink Classes

**Error**: `ClassNotFoundException: AuronExecNodeGraphProcessor`

**Cause**: Flink 1.18-SNAPSHOT JARs not in Maven local or missing Auron integration

**Solution**:
1. Rebuild Flink: `./build-all.sh --flink-clean --auron-only`
2. Verify JARs: Check `~/.m2/repository/org/apache/flink/`

### Wrong Flink Version

**Error**: Version mismatch errors at runtime

**Solution**: Clean and rebuild both:
```bash
./build-all.sh --clean
```

### Native Library Not Found

**Error**: `Cannot find libauron.dylib`

**Solution**: Rebuild native library:
```bash
cd /Users/vsowrira/git/auron
./build-flink.sh clean
```

### Architecture Mismatch

**Error**: `mach-o file, but is an incompatible architecture`

**Solution**: Ensure Java matches system architecture:
- Apple Silicon (M1/M2): Use ARM64 Java
- Intel Mac: Use x86_64 Java

### Tests Fail - ClassLoader Issues

**Error**: `Trying to access closed classloader`

**Cause**: Flink mini-cluster limitation with multiple queries

**Solution**: Use single-query tests (groupby, parallel, mvp) or run tests separately:
```bash
./run-example.sh groupby   # Single query, works reliably
```

## Build System Overview

### Directory Structure

```
/Users/vsowrira/git/flink/
├── auron-flink-1.18-integration (branch)
├── flink-table/flink-table-api-java/
│   └── OptimizerConfigOptions.java (Auron config)
└── flink-table/flink-table-planner/
    ├── AuronExecNodeGraphProcessor.java (pattern detection)
    └── AuronBatchExecNode.java (wrapper node)

/Users/vsowrira/git/auron/
├── build-all.sh (unified build script)
├── build-flink.sh (Auron-only build)
├── auron-flink-extension/auron-flink-planner/
│   ├── AuronExecNodeConverter.java (converts exec nodes)
│   ├── AuronTransformationFactory.java (creates transformations)
│   ├── AuronBatchExecutionWrapperOperator.java (executes native code)
│   ├── run-example.sh (test runner)
│   └── run-e2e-test-final.sh (integration tests)
└── native-engine/src-flink/ (Rust native library)
```

### Dependency Flow

```
1. Build Flink 1.18-SNAPSHOT
   ↓
2. Install to Maven Local Repository
   ~/.m2/repository/org/apache/flink/
   ↓
3. Build Auron with -Pflink-1.18
   ↓
4. Auron pulls Flink 1.18-SNAPSHOT from Maven Local
   ↓
5. Creates auron-flink-planner JAR with converters
   ↓
6. Tests run with both JARs on classpath
   ↓
7. Flink detects Auron classes via reflection
   ↓
8. Automatic conversion happens at query execution time
```

## Development Workflow

### Typical Iteration Cycle

1. **First time setup** (once):
   ```bash
   ./build-all.sh
   ```

2. **Modify Flink integration code** (in `/Users/vsowrira/git/flink`):
   ```bash
   ./build-all.sh --flink-only --skip-tests
   ```

3. **Modify Auron converter code** (in `/Users/vsowrira/git/auron`):
   ```bash
   ./build-all.sh --auron-only
   ```

4. **Test changes**:
   ```bash
   cd auron-flink-extension/auron-flink-planner
   ./run-example.sh groupby
   ```

5. **Clean rebuild** (if things break):
   ```bash
   ./build-all.sh --clean
   ```

## Performance Tips

- **Skip tests** for faster iteration: `--skip-tests`
- **Build only what changed**: Use `--flink-only` or `--auron-only`
- **Use incremental builds**: Avoid `--clean` unless necessary
- **First build is slow**: Flink takes 10-15 min, but subsequent builds are much faster (2-3 min)

## Additional Resources

- **Quick Start Guide**: See [QUICKSTART-FLINK.md](QUICKSTART-FLINK.md)
- **Flink Integration Details**: See `/Users/vsowrira/git/flink/AURON_INTEGRATION_1.18.md`
- **Development Guidelines**: See [CLAUDE.md](CLAUDE.md)
- **Main README**: See [README.md](README.md)

## Summary of Build Commands

| Task | Command | Time |
|------|---------|------|
| Build everything (first time) | `./build-all.sh` | 15-20 min |
| Build everything (incremental) | `./build-all.sh` | 3-5 min |
| Build only Flink | `./build-all.sh --flink-only` | 10-15 min |
| Build only Auron | `./build-all.sh --auron-only` | 3-5 min |
| Fast build (no tests) | `./build-all.sh --skip-tests` | 5-8 min |
| Clean rebuild everything | `./build-all.sh --clean` | 20-25 min |
| Run examples | `cd auron-flink-extension/auron-flink-planner && ./run-example.sh groupby` | < 10 sec |
| Run integration tests | `cd auron-flink-extension/auron-flink-planner && ./run-e2e-test-final.sh` | < 10 sec |
