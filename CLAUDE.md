# Auron Development Guidelines for Claude

## Java Version

**Always use Java 17 for building and testing Auron:**

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
```

While the project compiles to Java 8 bytecode for compatibility, Maven plugins (especially spotless) require Java 11+. Java 17 is recommended for all development work.

## Building the Project

Build Auron with Flink 1.18 profile:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
./build/apache-maven-3.9.12/bin/mvn clean install -DskipTests \
  -Pflink-1.18 -Pspark-3.5 -Pscala-2.12
```

## Testing

Run tests from the Auron root directory with `-am` flag to ensure all dependencies are resolved:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
./build/apache-maven-3.9.12/bin/mvn test -pl auron-flink-extension/auron-flink-planner -am \
  -Pflink-1.18 -Pspark-3.5 -Pscala-2.12
```

## Flink-Auron Integration

The Flink-Auron integration is located in `auron-flink-extension/auron-flink-planner` and works with Flink 1.18-SNAPSHOT built from the `auron-integration` branch.

### Key Integration Points

1. **Flink Planner Integration**: `/Users/vsowrira/git/flink/flink-table/flink-table-planner/src/main/java/org/apache/flink/table/planner/plan/nodes/exec/processor/AuronExecNodeGraphProcessor.java`
2. **Auron Converter**: `auron-flink-extension/auron-flink-planner/src/main/java/org/apache/auron/flink/planner/AuronExecNodeConverter.java`
3. **Verification Test**: `auron-flink-extension/auron-flink-planner/src/test/java/org/apache/auron/flink/planner/AuronIntegrationVerificationTest.java`
4. **Manual E2E Test**: `auron-flink-extension/auron-flink-planner/src/test/java/org/apache/auron/flink/planner/AuronEndToEndManualTest.java`

### Dependency Strategy

The project uses a hybrid dependency approach:
- Most Flink dependencies use version `1.18.1` (from Maven Central)
- Flink planner uses version `1.18-SNAPSHOT` (locally built from auron-integration branch)
- This is controlled by the `${flink.planner.version}` property in the POM

### Running End-to-End Tests

#### AuronExecutionVerificationTest

The main end-to-end test that **ACTUALLY EXECUTES** queries to verify Auron integration:
- Creates real Parquet test data
- Executes queries (not just explain) with Auron enabled/disabled
- Shows INFO-level logs from `AuronExecNodeGraphProcessor` during execution
- Look for log message: **"Converted BatchPhysicalTableSourceScan to Auron native execution"**
- Processes and displays actual query results

This test provides **definitive proof** that Auron conversion is happening by:
1. Showing the conversion log message during execution
2. Actually running the query through Auron's native execution engine

#### Running the Test

##### Option 1: Command Line (Recommended)

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
cd auron-flink-extension/auron-flink-planner

# Run the test
./run-e2e-test.sh
```

**Performance**:
- First run: Takes ~30 seconds to copy all dependencies to `target/lib`
- Subsequent runs: Start immediately (< 1 second)

##### Option 2: Run from IDE

After compiling test classes, run from your IDE:

1. **First, compile all classes** (one-time setup):
   ```bash
   export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
   ./build/apache-maven-3.9.12/bin/mvn test-compile \
     -pl auron-flink-extension/auron-flink-planner -am \
     -Pflink-1.18 -Pspark-3.5 -Pscala-2.12
   ```

2. **Reload your IDE** (IntelliJ: File → Reload All from Disk; VS Code: Developer → Reload Window)

3. **Run the test**:
   - Navigate to: `auron-flink-extension/auron-flink-planner/src/test/java/org/apache/auron/flink/planner/AuronExecutionVerificationTest.java`
   - Right-click and select "Run 'AuronExecutionVerificationTest.main()'"

#### Native Library Requirements

The test requires Auron's native library with **Flink feature enabled** (to exclude Spark dependencies).

**For ARM64 Java (Java 17+ on Apple Silicon)**:
```bash
cd /Users/vsowrira/git/auron
./dev/mvn-build-helper/build-native.sh release flink
cp native-engine/_build/release/libauron.dylib auron-flink-extension/auron-flink-planner/target/classes/
```

**For x86_64 Java (Java 8)**:
```bash
cd /Users/vsowrira/git/auron
CARGO_BUILD_TARGET=x86_64-apple-darwin ./dev/mvn-build-helper/build-native.sh release flink
cp target/x86_64-apple-darwin/release/libauron.dylib auron-flink-extension/auron-flink-planner/target/classes/
```

**Important**: The `flink` feature flag is required to disable Spark-specific JNI classes. The library must be copied to `target/classes/` to be available on the classpath for Flink's worker tasks.

**Build time**: First build takes ~4-6 minutes, subsequent builds are cached.

#### Expected Output

Look for these **key log messages** during execution:

```
INFO  o.a.f.t.p.p.n.e.p.AuronExecNodeGraphProcessor - Auron native execution is available and will be used for supported operators
...
INFO  o.a.f.t.p.p.n.e.p.AuronExecNodeGraphProcessor - Converted BatchPhysicalTableSourceScan to Auron native execution: ...
```

If you see these messages: **✅ Auron integration is working!** The query is actually being executed through Auron's native engine.

The test also shows:
- Test data creation
- Query execution WITH Auron (watch for conversion logs)
- Query execution WITHOUT Auron (for comparison)
- Actual query results (first 5 rows)

#### Current Status (January 2026)

**What Works**:
- ✅ Auron conversion during query planning
- ✅ BATCH mode execution (boundedness correctly set to BOUNDED)
- ✅ Native library loading with `flink` feature (Spark dependencies excluded)
- ✅ Auron native execution initializes successfully
- ✅ All 14 parallel tasks initialize Auron native execution
- ✅ Hadoop FileSystem registered successfully
- ✅ End-to-end integration working

**Fixed Issues**:
- ✅ Boundedness detection (Added explicit `setBoundedness(Boundedness.BOUNDED)` in `AuronTransformationFactory`)
- ✅ Spark dependency errors (Native library built with `flink` feature flag)
- ✅ Library loading (Copied to `target/classes/` for classpath access)
- ✅ Java 17 compatibility (Added `--add-opens` flags for Arrow memory access)
