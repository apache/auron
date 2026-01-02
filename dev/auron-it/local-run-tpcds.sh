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
set -ex

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AURON_DIR="$SCRIPT_DIR/../.."

# TPC-DS Dataset Preparation
# 1. Download pre-generated 1GB TPC-DS dataset: https://github.com/auron-project/tpcds_1g
# 2. OR generate via databricks/tpcds-kit: https://github.com/databricks/tpcds-kit
# Place data in a readable directory (e.g., /tmp/tpcds_1g) post-generation.

# Run TPC-DS Benchmark (Vanilla Spark vs Auron)
# - Executes specified TPC-DS queries (all if --query-filter unspecified) with Vanilla Spark/Auron
# - Verifies query result consistency (enabled by default) and reports execution time and speedup (Vanilla/Auron)
$SCRIPT_DIR/run-it.sh \
  --type tpcds \
  --data-location /tmp/tpcds_1g \
  --query-filter q1,q2,q3

# Run Only Auron plan stability check
# - Validates Auron physical plans against golden files.
$SCRIPT_DIR/run-it.sh \
  --type tpcds \
  --data-location /tmp/tpcds_1g \
  --query-filter q1,q2,q3 \
  --auron-only \
  --plan-check

# Regenerate Golden Files for Plan Stability Check
# - Rewrites golden plan files using Auron plans for the current Spark version
$SCRIPT_DIR/run-it.sh \
  --type tpcds \
  --data-location /tmp/tpcds_1g \
  --query-filter q1,q2,q3 \
  --auron-only \
  --regen-golden
