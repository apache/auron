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

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <data_path> <table_schema> <sql_query>"
    echo ""
    echo "This runs the query with both Auron and Flink native and verifies:"
    echo "  1. Row counts match"
    echo "  2. All row contents are identical"
    echo "  3. Performance comparison"
    echo ""
    echo "Example:"
    echo "  ./compare-query.sh /tmp/data \\"
    echo "    'id BIGINT, name STRING, amount DOUBLE' \\"
    echo "    'SELECT * FROM sales WHERE amount > 1000 LIMIT 100'"
    exit 1
fi

DATA_PATH="$1"
TABLE_SCHEMA="$2"
SQL_QUERY="$3"

if [ ! -d "$DATA_PATH" ]; then
    echo "❌ Data not found at $DATA_PATH"
    exit 1
fi

if ! curl -s http://localhost:8081/overview > /dev/null 2>&1; then
    echo "❌ Flink cluster is not running"
    exit 1
fi

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

# Compile and package if needed
JAR_FILE="target/compare-query-runner.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "Building comparison tool..."

    javac -cp "target/classes:$(ls /Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/lib/*.jar | tr '\n' ':')" \
        src/main/java/org/apache/auron/flink/planner/CompareQueryRunner.java \
        -d target/classes/

    cd target/classes
    jar cf ../compare-query-runner-temp.jar org/apache/auron/flink/planner/CompareQueryRunner*.class
    cd "$PROJECT_DIR"

    cd target
    mkdir -p comp-temp
    cd comp-temp
    unzip -qo ../compare-query-runner-temp.jar
    unzip -qo /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-assembly/target/auron-flink-assembly-7.0.0-SNAPSHOT.jar
    jar cf ../compare-query-runner.jar .
    cd ..
    rm -rf comp-temp compare-query-runner-temp.jar
    cd "$PROJECT_DIR"

    echo "✅ Comparison tool ready"
    echo ""
fi

/Users/vsowrira/git/flink/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/bin/flink run \
    -c org.apache.auron.flink.planner.CompareQueryRunner \
    "$JAR_FILE" \
    "$DATA_PATH" "$TABLE_SCHEMA" "$SQL_QUERY"
