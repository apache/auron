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

parse_args_for_spark_submit() {
  SPARK_CONF=()
  ARGS=()
  while [ ! -z "$1" ]; do
    if [[ "$1" =~ ^--master= ]]; then
      SPARK_CONF+=($1)
    elif [ "$1" == "--conf" ]; then
      shift
      SPARK_CONF+=("--conf $1")
    else
      ARGS+=($1)
    fi
    shift
  done
}

# 调用示例（和你原来完全一样）
parse_args_for_spark_submit --conf spark.shuffle.manager=org.apache.spark.sql.execution.auron.shuffle.celeborn.AuronCelebornShuffleManager --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.celeborn.client.spark.shuffle.writer=hash --conf spark.celeborn.client.push.replicate.enabled=false  --data-location dev/tpcds_1g


echo "SPARK_CONF-------"
printf '%s\n' "${SPARK_CONF[@]}"   # 改成这种方式才能正确显示带空格的元素
echo "ARGS-------"
printf '%s' "${ARGS[@]}"