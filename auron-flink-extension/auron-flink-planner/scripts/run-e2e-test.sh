#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Robust E2E test runner that copies all dependencies first
# This ensures all Flink jars (including provided scope) are available
#
# Usage:
#   ./run-e2e-test.sh                    # Runs AuronExecutionVerificationTest

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
AURON_ROOT="$PROJECT_DIR/../.."

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

# Check for native library
NATIVE_LIB="$PROJECT_DIR/target/classes/libauron.dylib"
if [ ! -f "$NATIVE_LIB" ]; then
    echo "Native library not found. Building from scratch..."
    cd "$AURON_ROOT"
    ./build/apache-maven-3.9.12/bin/mvn clean install -DskipTests \
      -Pflink-1.18 -Pspark-3.5 -Pscala-2.12
    cd "$PROJECT_DIR"
    echo ""
fi

# Check if test class file exists
TEST_CLASS_FILE="$PROJECT_DIR/target/test-classes/org/apache/auron/flink/planner/AuronExecutionVerificationTest.class"
if [ ! -f "$TEST_CLASS_FILE" ]; then
    echo "Test class not compiled. Compiling..."
    cd "$AURON_ROOT"
    ./build/apache-maven-3.9.12/bin/mvn test-compile \
      -pl auron-flink-extension/auron-flink-planner -am \
      -Pflink-1.18 -Pspark-3.5 -Pscala-2.12
    cd "$PROJECT_DIR"
    echo ""
fi

# Copy dependencies if not already done
if [ ! -d "$PROJECT_DIR/target/lib" ] || [ -z "$(ls -A $PROJECT_DIR/target/lib 2>/dev/null)" ]; then
    echo "Copying dependencies to target/lib (one-time setup)..."
    cd "$AURON_ROOT"
    ./build/apache-maven-3.9.12/bin/mvn dependency:copy-dependencies \
      -pl auron-flink-extension/auron-flink-planner \
      -DoutputDirectory=target/lib \
      -Pflink-1.18 -Pspark-3.5 -Pscala-2.12
    cd "$PROJECT_DIR"
    echo ""
fi

# Build classpath
CP="$PROJECT_DIR/target/classes:$PROJECT_DIR/target/test-classes:$CP"
for jar in $PROJECT_DIR/target/lib/*.jar; do
    CP="$CP:$jar"
done

# Check for log config
LOG_CONFIG="$PROJECT_DIR/log4j2-auron-test.properties"
if [ -f "$LOG_CONFIG" ]; then
    echo "Using log configuration: log4j2-auron-test.properties"
    LOG_OPTS="-Dlog4j.configurationFile=file:$LOG_CONFIG"
else
    LOG_OPTS=""
fi

# Run the test
echo "Running $TEST_CLASS..."
echo ""

java $LOG_OPTS \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    -cp "$CP" \
    "org.apache.auron.flink.planner.$TEST_CLASS"
