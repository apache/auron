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

The decimal dictionary fixtures were written with parquet-mr 1.13.1
`ExampleParquetWriter`, `PARQUET_1_0`, dictionary encoding enabled, and no
compression. Their schemas are:

```
message schema { required int32 d (DECIMAL(9,2)); }
message schema { required int64 d (DECIMAL(18,2)); }
```

Each file has one row group containing the unscaled values `[-100, 0, 100, 200]`
repeated 250 times. Both dictionary and data pages use `PLAIN_DICTIONARY` in
the footer's page encoding statistics; column encodings are
`[PLAIN_DICTIONARY, BIT_PACKED]`.

These files exercise Auron's legacy dictionary extraction path. Modern Rust
writer output does not enter that path; the test generates plain and modern
dictionary files as controls.

The scale and row-group fixtures use the same writer with these schemas:

```
message schema { optional int32 d (DECIMAL(9,0)); }
message schema { optional int64 d (DECIMAL(18,18)); }
```

`decimal-int32-scale0-dictionary.parquet` and
`decimal-int64-scale18-dictionary.parquet` each contain 600 rows in six row
groups, including 100 nulls. Let `limit = 10^precision - 1`. The first 300 rows
repeat `[-limit, -100, 0, 100, limit, null]`; the last 300 repeat
`[-limit, -150, 0, 150, limit, null]`. They were written with row-group size 1
and page size 1024; parquet-mr checks the row-group size every 100 rows.
All row groups use the legacy dictionary encoding.

The tests check zero scale, scale equal to precision, both precision limits,
nulls, and selective pruning across row groups. The unscaled value `1` lies
within every group's min/max range but is absent from every dictionary, so
discarding all groups for that value verifies dictionary pruning.
