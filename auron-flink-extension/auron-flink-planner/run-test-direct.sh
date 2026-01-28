#!/bin/bash

# Direct test runner that builds classpath from installed Maven artifacts
# Works with main()-based tests by invoking java directly

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
AURON_ROOT="$SCRIPT_DIR/../.."

# Test class to run (default or from argument)
TEST_CLASS="${1:-AuronExecutionVerificationTest}"

echo "=========================================="
echo "Running: $TEST_CLASS"
echo "=========================================="
echo ""

# Check Java version
if [ -z "$JAVA_HOME" ]; then
    echo "ERROR: JAVA_HOME is not set"
    echo "Please set JAVA_HOME to Java 17:"
    echo "export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home"
    exit 1
fi

echo "Using JAVA_HOME: $JAVA_HOME"
echo ""

# Build classpath by getting Maven dependencies for the test scope
echo "Building classpath from Maven..."
cd "$AURON_ROOT"

# Get dependency classpath (this works after mvn install)
MAVEN_CP=$(./build/apache-maven-3.9.12/bin/mvn \
    -pl auron-flink-extension/auron-flink-planner \
    dependency:build-classpath \
    -DincludeScope=test \
    -Dmdep.outputFile=/dev/stdout \
    -Pflink-1.18 -Pspark-3.5 -Pscala-2.12 \
    -q 2>&1 | grep -v "^\[" | head -1)

if [ -z "$MAVEN_CP" ]; then
    echo "ERROR: Failed to get Maven classpath"
    echo "Make sure you've run: mvn install -DskipTests -Drat.skip=true -Pflink-1.18"
    exit 1
fi

# Build complete classpath: test classes + main classes + Maven dependencies
CP="$SCRIPT_DIR/target/test-classes"
CP="$CP:$SCRIPT_DIR/target/classes"
CP="$CP:$AURON_ROOT/auron-flink-extension/auron-flink-runtime/target/classes"
CP="$CP:$AURON_ROOT/auron-core/target/classes"
CP="$CP:$AURON_ROOT/hadoop-shim/target/classes"
CP="$CP:$AURON_ROOT/dev/mvn-build-helper/proto/target/classes"
CP="$CP:$MAVEN_CP"

echo "Classpath configured"
echo ""

# Set up native library path
NATIVE_LIB_DIR="$AURON_ROOT/target/release"
if [ ! -f "$NATIVE_LIB_DIR/libauron.dylib" ]; then
    echo "WARNING: Native library not found at $NATIVE_LIB_DIR/libauron.dylib"
    echo "Run: mvn install -DskipTests -Drat.skip=true -Pflink-1.18"
    echo ""
fi

export LD_LIBRARY_PATH="$NATIVE_LIB_DIR:${LD_LIBRARY_PATH:-}"
export DYLD_LIBRARY_PATH="$NATIVE_LIB_DIR:${DYLD_LIBRARY_PATH:-}"

# Java 17+ requires additional JVM flags for Arrow
JAVA_VERSION=$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
if [ "$JAVA_VERSION" -ge 9 ]; then
    JVM_ARGS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
else
    JVM_ARGS=""
fi

# Add library path
JVM_ARGS="$JVM_ARGS -Djava.library.path=$NATIVE_LIB_DIR"
JVM_ARGS="$JVM_ARGS -Denv.java.opts=-Djava.library.path=$NATIVE_LIB_DIR"

# Add log configuration if available
LOG_CONFIG="$SCRIPT_DIR/log4j2-auron-test.properties"
if [ -f "$LOG_CONFIG" ]; then
    JVM_ARGS="$JVM_ARGS -Dlog4j.configurationFile=file://$LOG_CONFIG"
    echo "Using log configuration: log4j2-auron-test.properties"
fi

echo "Native library path: $NATIVE_LIB_DIR"
echo ""
echo "=========================================="
echo "Executing Test"
echo "=========================================="
echo ""

# Run the test
"$JAVA_HOME/bin/java" \
    $JVM_ARGS \
    -cp "$CP" \
    org.apache.auron.flink.planner.$TEST_CLASS

EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Test Completed Successfully"
else
    echo "❌ Test Failed (exit code: $EXIT_CODE)"
fi
echo "=========================================="

exit $EXIT_CODE
