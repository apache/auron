<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

`integer-dictionary.parquet` was written with parquet-mr 1.13.1
`ExampleParquetWriter`, `PARQUET_1_0`, dictionary encoding enabled, and no
compression. Its schema is `message schema { required int32 x; }`.

It contains three row groups of 100 rows each. The groups alternate between
`[-10, -1]`, `[0, 10]`, and `[5, 10]`, respectively. It was written with
`withRowGroupSize(1)` and `withPageSize(1024)`; parquet-mr checks the row-group
size every 100 rows.

Both dictionary and data pages use `PLAIN_DICTIONARY` in the footer's page
encoding statistics. This exercises Auron's legacy dictionary extraction path;
modern Rust writer output does not enter that path.

For `x = 5`, min/max statistics prune the first group, dictionary filtering
prunes the second group, and the third group is retained. The regression test
checks these decisions, the metrics, and matching rows against an unpruned read.
