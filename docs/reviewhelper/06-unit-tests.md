# Commit 6: Unit Tests

**Commit**: `[AURON #1851] Add FlinkArrowReader unit tests`

## What This Commit Does

Adds `FlinkArrowReaderTest` with 21 test methods covering all 17 supported types, null handling, edge cases, and integration scenarios.

## File to Review (1)

`auron-flink-runtime/src/test/java/org/apache/auron/flink/arrow/FlinkArrowReaderTest.java`

## Test Coverage

### Per-type tests (17 tests — values + nulls for each type)
| Test | Type | Key Assertions |
|------|------|----------------|
| `testBooleanVector` | BOOLEAN | true/false via `getBoolean`, null via `isNullAt` |
| `testTinyIntVector` | TINYINT | byte values + null |
| `testSmallIntVector` | SMALLINT | short values + null |
| `testIntVector` | INTEGER | int values including MAX_VALUE + null |
| `testBigIntVector` | BIGINT | long values including MAX_VALUE + null |
| `testFloatVector` | FLOAT | float values including NaN + null |
| `testDoubleVector` | DOUBLE | double values including MAX_VALUE + null |
| `testVarCharVector` | VARCHAR | string bytes + null + empty string |
| `testVarBinaryVector` | VARBINARY | binary bytes + null + empty |
| `testDecimalVector` | DECIMAL | compact path (p=10) + wide path (p=20) + null |
| `testDateVector` | DATE | epoch days + null |
| `testTimeVector` | TIME | micros→millis conversion + null |
| `testTimestampVector` | TIMESTAMP | micros→TimestampData (millis + nanos) + null |
| `testTimestampLtzVector` | TIMESTAMP_LTZ | same as above with timezone |
| `testArrayVector` | ARRAY | nested int array + null + empty |
| `testMapVector` | MAP | key-value pairs + null + empty |
| `testRowVector` | ROW | nested struct + null |

### Integration tests (4 tests)
| Test | What It Validates |
|------|-------------------|
| `testMultiColumnBatch` | 3-column (int, varchar, boolean) batch, 3 rows |
| `testEmptyBatch` | 0-row batch → `getRowCount() == 0` |
| `testResetWithNewRoot` | `reset()` swaps batch, verifies new values |
| `testUnsupportedTypeThrows` | `RawType` → `UnsupportedOperationException` |

## What to Look For

- Each test creates Arrow vectors manually, populates values, wraps in `VectorSchemaRoot`, creates `FlinkArrowReader`, and asserts read values match
- Resource cleanup: `try-with-resources` for `BufferAllocator`, explicit `close()` for reader and root
- Decimal test verifies both compact (p≤18, uses `fromUnscaledLong`) and wide (p>18, uses `fromBigDecimal`) paths
