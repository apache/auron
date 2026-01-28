#!/bin/bash

# Test runner for parallelism > 1 (AuronParallelExecutionTest)
# Verifies that file splitting works correctly and data is not duplicated

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
AURON_ROOT="$SCRIPT_DIR/../.."

TEST_CLASS="AuronParallelExecutionTest"

echo "=========================================="
echo "Running Auron Parallel Execution Test"
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

# Check if native library exists
NATIVE_LIB="$AURON_ROOT/target/release/libauron.dylib"
if [ ! -f "$NATIVE_LIB" ]; then
    echo "Native library not found. Building from scratch..."
    cd "$AURON_ROOT"
    ./build/apache-maven-3.9.12/bin/mvn clean install -DskipTests \
        -Pflink-1.18 -Pspark-3.5 -Pscala-2.12
    echo ""
fi

# Ensure test classes are compiled (skipping native library rebuild)
TEST_CLASS_FILE="$SCRIPT_DIR/target/test-classes/org/apache/auron/flink/planner/$TEST_CLASS.class"
if [ ! -f "$TEST_CLASS_FILE" ]; then
    echo "Compiling test classes only (skipping native rebuild with -DskipBuildNative)..."
    cd "$AURON_ROOT"
    ./build/apache-maven-3.9.12/bin/mvn test-compile \
        -pl auron-flink-extension/auron-flink-planner -am \
        -Pflink-1.18 -Pspark-3.5 -Pscala-2.12 \
        -DskipBuildNative=true
    echo ""
else
    echo "Test classes already compiled, skipping compilation."
    echo ""
fi

# Build classpath using Maven
echo "Building classpath from Maven..."
cd "$AURON_ROOT"
CP=$(./build/apache-maven-3.9.12/bin/mvn \
    -pl auron-flink-extension/auron-flink-planner -am \
    -q dependency:build-classpath \
    -Dmdep.outputFile=/dev/stdout \
    -Pflink-1.18 -Pspark-3.5 -Pscala-2.12)

# Add project classes
CP="$SCRIPT_DIR/target/classes:$SCRIPT_DIR/target/test-classes:$CP"

# Add proto-generated classes
PROTO_CLASSES="$AURON_ROOT/dev/mvn-build-helper/proto/target/classes"
if [ -d "$PROTO_CLASSES" ]; then
    CP="$CP:$PROTO_CLASSES"
fi

# Add Auron dependency modules
CP="$CP:$AURON_ROOT/auron-core/target/classes"
CP="$CP:$AURON_ROOT/hadoop-shim/target/classes"
CP="$CP:$AURON_ROOT/auron-flink-extension/auron-flink-runtime/target/classes"

echo "Classpath configured successfully"
echo ""
echo "=========================================="
echo "Starting Parallel Execution Test"
echo "=========================================="
echo ""

# Set up native library path
NATIVE_LIB_DIR="$AURON_ROOT/target/release"
if [ ! -f "$NATIVE_LIB_DIR/libauron.dylib" ]; then
    echo "WARNING: Native library not found at $NATIVE_LIB_DIR/libauron.dylib"
    echo ""
fi

# Set library path in environment
export LD_LIBRARY_PATH="$NATIVE_LIB_DIR:${LD_LIBRARY_PATH:-}"
export DYLD_LIBRARY_PATH="$NATIVE_LIB_DIR:${DYLD_LIBRARY_PATH:-}"

# Java 17+ requires additional JVM flags for Arrow memory access
JAVA_VERSION=$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
if [ "$JAVA_VERSION" -ge 9 ]; then
    ARROW_FLAGS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
else
    ARROW_FLAGS=""
fi

# Run the test
LOG_CONFIG="$SCRIPT_DIR/log4j2-auron-test.properties"
if [ -f "$LOG_CONFIG" ]; then
    echo "Using log configuration: log4j2-auron-test.properties"
    echo "Native library path: $NATIVE_LIB_DIR"
    echo ""
    "$JAVA_HOME/bin/java" \
        $ARROW_FLAGS \
        -Djava.library.path="$NATIVE_LIB_DIR" \
        -Denv.java.opts="-Djava.library.path=$NATIVE_LIB_DIR" \
        -Dlog4j.configurationFile="file://$LOG_CONFIG" \
        -cp "$CP" \
        org.apache.auron.flink.planner.$TEST_CLASS
else
    echo "Running without explicit logging config"
    echo "Native library path: $NATIVE_LIB_DIR"
    echo ""
    "$JAVA_HOME/bin/java" \
        $ARROW_FLAGS \
        -Djava.library.path="$NATIVE_LIB_DIR" \
        -Denv.java.opts="-Djava.library.path=$NATIVE_LIB_DIR" \
        -cp "$CP" \
        org.apache.auron.flink.planner.$TEST_CLASS
fi

EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Parallel Execution Test Completed Successfully"
else
    echo "❌ Parallel Execution Test Failed with exit code: $EXIT_CODE"
fi
echo "=========================================="

exit $EXIT_CODE
