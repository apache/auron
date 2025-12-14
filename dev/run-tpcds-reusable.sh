#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -ex
#
# Shell script for validating TPCDS query results
export SPARK_HOME=/Users/yew1eb/workspaces/spark-3.5.7-bin-hadoop3

if [ -z "${SPARK_HOME}" ]; then
  echo "env SPARK_HOME not defined" 1>&2
  exit 1
fi

# Determine the current working directory
_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $_DIR

extrasparkconf="
  --conf spark.sql.extensions=org.apache.spark.sql.auron.AuronSparkSessionExtension
  --conf spark.shuffle.manager=org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager"

queries='["q1,q2,q3,q4,q5,q6,q7,q8,q9"]'
export SPARK_TPCDS_DATA="dev/tpcds_1g"
export QUERY_FILTER="${queries}"
./build/mvn -B -Ppre -Pspark-3.5 -Pscala-2.12 -Dsuites=org.apache.spark.sql.AuronTPCDSSuite test -pl spark-extension-shims-spark