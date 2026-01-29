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

# Run AuronExecutionVerificationTest directly with Java
# This bypasses Maven's exec:java plugin issues

set -e

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
cd "$(dirname "$0")/../.."

echo "=============================================="
echo "Running AuronExecutionVerificationTest"
echo "=============================================="
echo ""
echo "This will generate 100k rows of Parquet test data"
echo "and execute queries with/without Auron enabled."
echo ""

# Build classpath from Maven
echo "Building classpath..."
CLASSPATH=$(./build/apache-maven-3.9.12/bin/mvn dependency:build-classpath \
  -pl auron-flink-extension/auron-flink-planner -am \
  -Pflink-1.18 -Pscala-2.12 \
  -DincludeScope=test -q -Dmdep.outputFile=/dev/stdout)

# Add compiled classes and Auron runtime JAR
CLASSPATH="$CLASSPATH:auron-flink-extension/auron-flink-planner/target/test-classes:auron-flink-extension/auron-flink-planner/target/classes:auron-flink-extension/auron-flink-runtime/target/auron-flink-runtime-7.0.0-SNAPSHOT.jar"

# Run the test with JVM flags for Arrow
echo "Running test..."
echo ""
$JAVA_HOME/bin/java \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  -cp "$CLASSPATH" \
  org.apache.auron.flink.planner.AuronExecutionVerificationTest
