#!/bin/bash

# Robust E2E test runner that copies all dependencies first
# This ensures all Flink jars (including provided scope) are available
#
# Usage:
#   ./run-e2e-test-final.sh                    # Runs AuronExecutionVerificationTest

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
AURON_ROOT="$SCRIPT_DIR/../.."

# Use the main test class
TEST_CLASS="AuronExecutionVerificationTest"

echo "=========================================="
echo "Running Auron Flink Test: $TEST_CLASS"
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

# Ensure test classes are compiled
if [ ! -f "$SCRIPT_DIR/target/test-classes/org/apache/auron/flink/planner/AuronEndToEndManualTest.class" ]; then
    echo "Compiling test classes..."
    cd "$AURON_ROOT"
    ./build/apache-maven-3.9.12/bin/mvn test-compile \
        -pl auron-flink-extension/auron-flink-planner -am \
        -Pflink-1.18 -Pspark-3.5 -Pscala-2.12
    echo ""
fi

# Copy all dependencies (including provided scope) to target/lib
LIB_DIR="$SCRIPT_DIR/target/lib"
if [ ! -d "$LIB_DIR" ] || [ -z "$(ls -A $LIB_DIR 2>/dev/null)" ]; then
    echo "Copying dependencies (this happens once, takes ~30 seconds)..."
    cd "$AURON_ROOT"
    ./build/apache-maven-3.9.12/bin/mvn \
        -pl auron-flink-extension/auron-flink-planner \
        dependency:copy-dependencies \
        -DincludeScope=test \
        -DoutputDirectory="$LIB_DIR" \
        -Pflink-1.18 -Pspark-3.5 -Pscala-2.12 \
        -q
    echo "Dependencies copied to $LIB_DIR"
    echo ""
fi

# Build classpath
echo "Building classpath..."
CP="$SCRIPT_DIR/target/classes"
CP="$CP:$SCRIPT_DIR/target/test-classes"

# Add proto-generated classes
PROTO_CLASSES="$AURON_ROOT/dev/mvn-build-helper/proto/target/classes"
if [ -d "$PROTO_CLASSES" ]; then
    CP="$CP:$PROTO_CLASSES"
    echo "Added proto classes from: dev/mvn-build-helper/proto"
else
    echo "WARNING: Proto classes not found at $PROTO_CLASSES"
fi

# Add Auron dependency modules
CP="$CP:$AURON_ROOT/auron-core/target/classes"
CP="$CP:$AURON_ROOT/hadoop-shim/target/classes"
CP="$CP:$AURON_ROOT/auron-flink-extension/auron-flink-runtime/target/classes"

# Add all jars from lib directory
for jar in "$LIB_DIR"/*.jar; do
    if [ -f "$jar" ]; then
        CP="$CP:$jar"
    fi
done

echo "Classpath configured with $(ls -1 $LIB_DIR/*.jar 2>/dev/null | wc -l) dependency jars"
echo ""
echo "=========================================="
echo "Starting E2E Test"
echo "=========================================="
echo ""

# Set up native library path
# Use x86_64 build for compatibility with x86_64 JVMs (like Java 8)
NATIVE_LIB_DIR="$AURON_ROOT/target/x86_64-apple-darwin/release"
if [ ! -f "$NATIVE_LIB_DIR/libauron.dylib" ]; then
    echo "WARNING: Native library not found at $NATIVE_LIB_DIR/libauron.dylib"
    echo "The test will likely fail. To build it for x86_64, run:"
    echo "  cd $AURON_ROOT && CARGO_BUILD_TARGET=x86_64-apple-darwin ./dev/mvn-build-helper/build-native.sh release"
    echo ""
fi

# Set library path in environment (for Flink workers)
export LD_LIBRARY_PATH="$NATIVE_LIB_DIR:${LD_LIBRARY_PATH:-}"
export DYLD_LIBRARY_PATH="$NATIVE_LIB_DIR:${DYLD_LIBRARY_PATH:-}"

# Java 17+ requires additional JVM flags for Arrow memory access
JAVA_VERSION=$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
if [ "$JAVA_VERSION" -ge 9 ]; then
    ARROW_FLAGS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
else
    ARROW_FLAGS=""
fi

# Run the test with logging configuration and native library path
LOG_CONFIG="$SCRIPT_DIR/log4j2-auron-test.properties"
if [ -f "$LOG_CONFIG" ]; then
    echo "Using log configuration: log4j2-auron-test.properties"
    echo "(This will show INFO logs from AuronExecNodeGraphProcessor)"
    echo "Native library path: $NATIVE_LIB_DIR"
    echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
    echo "DYLD_LIBRARY_PATH: $DYLD_LIBRARY_PATH"
    echo ""
    "$JAVA_HOME/bin/java" \
        $ARROW_FLAGS \
        -Djava.library.path="$NATIVE_LIB_DIR" \
        -Denv.java.opts="-Djava.library.path=$NATIVE_LIB_DIR" \
        -Dlog4j.configurationFile="file://$LOG_CONFIG" \
        -cp "$CP" \
        org.apache.auron.flink.planner.$TEST_CLASS
else
    echo "Log configuration not found, running without explicit logging config"
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
    echo "✅ E2E Test Completed Successfully"
else
    echo "❌ E2E Test Failed with exit code: $EXIT_CODE"
fi
echo "=========================================="

exit $EXIT_CODE
