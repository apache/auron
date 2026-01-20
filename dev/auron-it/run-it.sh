#!/usr/bin/env bash

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

set -exo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AURON_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

MVN_CMD="${AURON_DIR}/build/mvn"
export RUST_BACKTRACE=1
SPARK_VERSION="${SPARK_VERSION:-spark-3.5}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"
PROFILES="-P${SPARK_VERSION},scala-${SCALA_VERSION}"
PROJECT_VERSION="$("${MVN_CMD}" -f "${AURON_DIR}/pom.xml" -q ${PROFILES} help:evaluate -Dexpression=project.version -DforceStdout)"

AURON_SPARK_JAR="${AURON_SPARK_JAR:-${AURON_DIR}/dev/mvn-build-helper/assembly/target/auron-${SPARK_VERSION}_${SCALA_VERSION}-${PROJECT_VERSION}.jar}"
AURON_IT_JAR="${AURON_DIR}/dev/auron-it/target/auron-it-${PROJECT_VERSION}-jar-with-dependencies.jar"


if [[ -z "${SPARK_HOME:-}" ]]; then
  echo "ERROR: SPARK_HOME environment variable must be set"
  exit 1
fi

if [[ ! -f "${AURON_SPARK_JAR}" ]]; then
    echo "ERROR: Auron Spark JAR not found at: ${AURON_SPARK_JAR}"
    echo "Hint: Rebuild with: ./auron-build.sh"
    exit 1
fi

if [[ ! -f "$AURON_IT_JAR" ]]; then
  echo "INFO: Building missing Auron it jar..."
  pushd "${SCRIPT_DIR}"
  "${MVN_CMD}" ${PROFILES} package -DskipTests
  popd
fi

# Split input arguments into two parts: Spark confs and args
SPARK_CONF=()
ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --master=*)
      SPARK_CONF+=("$1") ;;
    --conf)
      shift
      SPARK_CONF+=("--conf" "$1") ;;
    *)
      ARGS+=("$1") ;;
  esac
  shift
done

exec $SPARK_HOME/bin/spark-submit \
  --driver-memory 10g \
  --conf spark.driver.memoryOverhead=4g \
  --conf spark.auron.memoryFraction=0.8 \
  --conf spark.driver.extraJavaOptions=-XX:+UseG1GC \
  --conf spark.ui.enabled=false \
  --jars "${AURON_SPARK_JAR}" \
  "${SPARK_CONF[@]}" \
  "${AURON_IT_JAR}" \
  "${ARGS[@]}"