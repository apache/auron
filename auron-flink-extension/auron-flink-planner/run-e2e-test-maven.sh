#!/bin/bash

# Simple test runner using Maven exec plugin
# This avoids all classpath issues by letting Maven handle it

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
AURON_ROOT="$SCRIPT_DIR/../.."

# Default test class
TEST_CLASS="${1:-AuronExecutionVerificationTest}"

echo "=========================================="
echo "Running Auron Test: $TEST_CLASS"
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

# Set up native library path
NATIVE_LIB_DIR="$AURON_ROOT/target/release"
export LD_LIBRARY_PATH="$NATIVE_LIB_DIR:${LD_LIBRARY_PATH:-}"
export DYLD_LIBRARY_PATH="$NATIVE_LIB_DIR:${DYLD_LIBRARY_PATH:-}"

# Java 17+ requires additional JVM flags
JAVA_VERSION=$("$JAVA_HOME/bin/java" -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
if [ "$JAVA_VERSION" -ge 9 ]; then
    JVM_ARGS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
else
    JVM_ARGS=""
fi

# Log config
LOG_CONFIG="$SCRIPT_DIR/log4j2-auron-test.properties"
if [ -f "$LOG_CONFIG" ]; then
    JVM_ARGS="$JVM_ARGS -Dlog4j.configurationFile=file://$LOG_CONFIG"
fi

# Library path
JVM_ARGS="$JVM_ARGS -Djava.library.path=$NATIVE_LIB_DIR -Denv.java.opts=-Djava.library.path=$NATIVE_LIB_DIR"

echo "Native library path: $NATIVE_LIB_DIR"
echo "JVM args: $JVM_ARGS"
echo ""

# Run via Maven exec plugin
cd "$AURON_ROOT"
./build/apache-maven-3.9.12/bin/mvn \
    -pl auron-flink-extension/auron-flink-planner \
    exec:java \
    -Dexec.mainClass="org.apache.auron.flink.planner.$TEST_CLASS" \
    -Dexec.classpathScope=test \
    -Dexec.args="" \
    -Pflink-1.18 -Pspark-3.5 -Pscala-2.12 \
    -Dexec.cleanupDaemonThreads=false

EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Test Completed Successfully"
else
    echo "❌ Test Failed with exit code: $EXIT_CODE"
fi
echo "=========================================="

exit $EXIT_CODE
