# Flink Cluster Deployment Guide - Auron Integration

This guide describes which JARs need to be deployed to a Flink cluster to run Auron-accelerated queries.

## Quick Summary

For a production Flink cluster, you need to deploy **3 JARs**:

1. **Flink Table Planner** (with Auron integration) - Replace existing Flink JAR
2. **Flink Table API Java** (with Auron config) - Replace existing Flink JAR
3. **Auron Flink Assembly** (fat JAR) - Add to Flink lib

## Detailed Deployment Steps

### Option 1: Full Assembly JAR (Recommended)

This is the simplest approach for production deployment.

#### Required JARs

| JAR | Location | Size | Purpose |
|-----|----------|------|---------|
| `flink-table-planner_2.12-1.18-SNAPSHOT.jar` | `~/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/` | ~21 MB | Flink planner with Auron integration |
| `flink-table-api-java-1.18-SNAPSHOT.jar` | `~/.m2/repository/org/apache/flink/flink-table-api-java/1.18-SNAPSHOT/` | ~1 MB | Flink Table API with Auron config |
| `auron-flink-assembly-7.0.0-SNAPSHOT.jar` | `/Users/vsowrira/git/auron/auron-flink-extension/auron-flink-assembly/target/` | ~139 MB | Auron with all dependencies + native library |

#### Deployment to Flink Cluster

```bash
# 1. Locate your Flink installation lib directory
FLINK_HOME=/path/to/flink
FLINK_LIB=$FLINK_HOME/lib

# 2. Backup existing Flink JARs
cp $FLINK_LIB/flink-table-planner*.jar $FLINK_LIB/flink-table-planner.jar.backup
cp $FLINK_LIB/flink-table-api-java*.jar $FLINK_LIB/flink-table-api-java.jar.backup

# 3. Replace Flink JARs with Auron-integrated versions
cp ~/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/flink-table-planner_2.12-1.18-SNAPSHOT.jar \
   $FLINK_LIB/

cp ~/.m2/repository/org/apache/flink/flink-table-api-java/1.18-SNAPSHOT/flink-table-api-java-1.18-SNAPSHOT.jar \
   $FLINK_LIB/

# 4. Remove old versions (if different names)
rm -f $FLINK_LIB/flink-table-planner_2.12-1.18.1.jar
rm -f $FLINK_LIB/flink-table-api-java-1.18.1.jar

# 5. Copy Auron assembly JAR
cp /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-assembly/target/auron-flink-assembly-7.0.0-SNAPSHOT.jar \
   $FLINK_LIB/
```

#### What's Included in Assembly JAR

The `auron-flink-assembly-7.0.0-SNAPSHOT.jar` contains:
- ✅ Auron core classes (`auron-core`)
- ✅ Auron Flink runtime (`auron-flink-runtime`)
- ✅ Auron Flink planner (`auron-flink-planner`)
- ✅ Native library (`libauron.dylib` for macOS, `libauron.so` for Linux)
- ✅ All dependencies (Arrow, Parquet, DataFusion bindings, etc.)

### Option 2: Individual JARs (Advanced)

If you need finer control or want to minimize duplication with existing Flink dependencies:

#### Required JARs

| JAR | Location | Size | Purpose |
|-----|----------|------|---------|
| `flink-table-planner_2.12-1.18-SNAPSHOT.jar` | `~/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/` | ~21 MB | Flink planner with Auron integration |
| `flink-table-api-java-1.18-SNAPSHOT.jar` | `~/.m2/repository/org/apache/flink/flink-table-api-java/1.18-SNAPSHOT/` | ~1 MB | Flink Table API with Auron config |
| `auron-core-7.0.0-SNAPSHOT.jar` | `/Users/vsowrira/git/auron/auron-core/target/` | ~27 KB | Auron core classes |
| `auron-flink-runtime-7.0.0-SNAPSHOT.jar` | `/Users/vsowrira/git/auron/auron-flink-extension/auron-flink-runtime/target/` | ~15 MB | Auron Flink runtime (includes dependencies) |
| `auron-flink-planner-7.0.0-SNAPSHOT.jar` | `/Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner/target/` | ~47 KB | Auron Flink planner |
| Native library | `/Users/vsowrira/git/auron/target/release/` | ~45 MB | `libauron.dylib` (macOS) or `libauron.so` (Linux) |

#### Additional Dependencies

The `auron-flink-runtime` JAR includes most dependencies, but verify these are in Flink's classpath:
- Apache Arrow (version compatible with Flink)
- Parquet libraries (if not already in Flink)
- Protocol Buffers runtime

#### Native Library Deployment

The native library needs special handling:

**Option A: Include in classpath** (JVM will extract it)
```bash
# The assembly JAR includes the native library
# Flink will extract it automatically to a temp directory
```

**Option B: Explicit path**
```bash
# Copy native library to a known location
cp /Users/vsowrira/git/auron/target/release/libauron.dylib /opt/auron/lib/

# Set in flink-conf.yaml
env.java.opts: -Djava.library.path=/opt/auron/lib
```

## Flink Configuration

Add these settings to `$FLINK_HOME/conf/flink-conf.yaml`:

```yaml
# Enable Auron optimizer
table.optimizer.auron.enabled: true

# Runtime mode (required for Auron)
execution.runtime-mode: BATCH

# Memory settings (recommended)
taskmanager.memory.managed.size: 1024m
taskmanager.memory.task.off-heap.size: 2048m

# Java options (required for Arrow on Java 17+)
env.java.opts: --add-opens=java.base/java.nio=ALL-UNNAMED
```

## Verification

After deployment, verify Auron is available:

### 1. Check JARs are Present

```bash
ls -lh $FLINK_HOME/lib/ | grep -E "auron|flink-table"
```

Expected output:
```
flink-table-planner_2.12-1.18-SNAPSHOT.jar
flink-table-api-java-1.18-SNAPSHOT.jar
auron-flink-assembly-7.0.0-SNAPSHOT.jar
```

### 2. Check Auron Classes

```bash
jar tf $FLINK_HOME/lib/flink-table-planner_2.12-1.18-SNAPSHOT.jar | grep AuronExecNodeGraphProcessor
```

Expected: `org/apache/flink/table/planner/plan/nodes/exec/processor/AuronExecNodeGraphProcessor.class`

### 3. Test Query

Submit a test query to verify Auron is working:

```sql
-- Create Parquet table
CREATE TABLE test_table (
    id BIGINT,
    name STRING,
    amount DOUBLE
) WITH (
    'connector' = 'filesystem',
    'path' = '/path/to/data',
    'format' = 'parquet'
);

-- This query should use Auron native execution
SELECT id, name, amount
FROM test_table
WHERE amount > 100.0
LIMIT 10;
```

Check the logs for Auron initialization:
```
[INFO] [auron::exec:70] - initializing JNI bridge
[INFO] [auron_jni_bridge::jni_bridge:491] - ==> FLINK MODE: Spark/Scala classes will be skipped
[INFO] [auron::rt:147] - start executing plan:
ParquetExec: limit=None, file_group=[...]
```

## Architecture-Specific Notes

### macOS (ARM64/M1)
- Native library: `libauron.dylib`
- Architecture: `arm64`
- Java must be ARM64 version

### macOS (Intel/x86_64)
- Native library: `libauron.dylib`
- Architecture: `x86_64`
- Java must be x86_64 version

### Linux (x86_64)
- Native library: `libauron.so`
- Architecture: `x86_64`
- Most common for production clusters

### Linux (ARM64/aarch64)
- Native library: `libauron.so`
- Architecture: `aarch64`
- For ARM-based servers

## Building for Production

### Build Assembly JAR

```bash
cd /Users/vsowrira/git/auron

# Build with all profiles
./build-all.sh
```

This creates:
- `auron-flink-assembly/target/auron-flink-assembly-7.0.0-SNAPSHOT.jar`

### Build Flink with Auron Integration

```bash
cd /Users/vsowrira/git/flink
git checkout auron-flink-1.18-integration

./mvnw clean install -DskipTests \
  -pl flink-table/flink-table-api-java,flink-table/flink-table-planner \
  -am -Pscala-2.12
```

This installs to Maven local:
- `~/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/`
- `~/.m2/repository/org/apache/flink/flink-table-api-java/1.18-SNAPSHOT/`

## Troubleshooting

### Issue: Native library not found

**Error**: `java.lang.UnsatisfiedLinkError: no auron in java.library.path`

**Solutions**:
1. Verify native library is in assembly JAR: `jar tf auron-flink-assembly*.jar | grep libauron`
2. Ensure correct architecture (ARM64 vs x86_64)
3. Set explicit path in `flink-conf.yaml`: `env.java.opts: -Djava.library.path=/path/to/lib`

### Issue: Auron classes not found

**Error**: `ClassNotFoundException: AuronExecNodeGraphProcessor`

**Solutions**:
1. Verify Flink planner JAR is replaced: `jar tf flink-table-planner*.jar | grep Auron`
2. Ensure Flink JARs are from `auron-flink-1.18-integration` branch
3. Restart Flink cluster after JAR changes

### Issue: Auron not converting queries

**Error**: Queries run but don't use Auron

**Solutions**:
1. Check configuration: `table.optimizer.auron.enabled: true` in `flink-conf.yaml`
2. Verify runtime mode: `execution.runtime-mode: BATCH`
3. Check query pattern is supported (Parquet scan + filter/projection)
4. Look for logs: `grep -i auron flink-taskmanager-*.log`

### Issue: Arrow memory errors

**Error**: `Failed to initialize MemoryUtil`

**Solution**: Add JVM flag in `flink-conf.yaml`:
```yaml
env.java.opts: --add-opens=java.base/java.nio=ALL-UNNAMED
```

## Summary Checklist

Before deploying to Flink cluster:

- [ ] Build Flink 1.18-SNAPSHOT with Auron integration
- [ ] Build Auron assembly JAR
- [ ] Backup existing Flink JARs in `$FLINK_HOME/lib`
- [ ] Copy 3 JARs to `$FLINK_HOME/lib`:
  - [ ] `flink-table-planner_2.12-1.18-SNAPSHOT.jar`
  - [ ] `flink-table-api-java-1.18-SNAPSHOT.jar`
  - [ ] `auron-flink-assembly-7.0.0-SNAPSHOT.jar`
- [ ] Configure `flink-conf.yaml`:
  - [ ] `table.optimizer.auron.enabled: true`
  - [ ] `execution.runtime-mode: BATCH`
  - [ ] `env.java.opts: --add-opens=java.base/java.nio=ALL-UNNAMED`
- [ ] Restart Flink cluster
- [ ] Test with sample query
- [ ] Verify Auron logs appear in taskmanager logs

## References

- **Build Guide**: [BUILD-GUIDE.md](BUILD-GUIDE.md)
- **Quick Start**: [QUICKSTART-FLINK.md](QUICKSTART-FLINK.md)
- **Flink Integration Details**: `/Users/vsowrira/git/flink/AURON_INTEGRATION_1.18.md`
