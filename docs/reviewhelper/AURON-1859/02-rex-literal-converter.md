# Commit 2: RexLiteralConverter — scalar literal conversion via Arrow IPC

## What it does

Implements `RexLiteralConverter`, converting Calcite `RexLiteral` values into Auron native `PhysicalExprNode` containing `ScalarValue` with Arrow IPC bytes. Supports numeric types (TINYINT through DOUBLE, DECIMAL), BOOLEAN, CHAR/VARCHAR, and typed NULL. Follows the Spark `NativeConverters.scala:409-430` pattern.

## Files to review

| File | Key details |
|------|-------------|
| `RexLiteralConverter.java` | `serializeToIpc()` creates single-element Arrow vector, serializes via `ArrowStreamWriter`. `isSupported()` checks type even for null literals (V1 bug fix). Per-call `RootAllocator` in try-with-resources. |
| `RexLiteralConverterTest.java` | 12 tests: getNodeClass + all 10 supported types (TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, BOOLEAN, STRING, NULL) + unsupported type rejection. |

## What to look for

1. Arrow IPC serialization: `allocateNew()` → set value → `setValueCount(1)` → `setRowCount(1)` → write via `ArrowStreamWriter(start/writeBatch/end)`. Resource management via try-with-resources.
2. Null literal type safety: `isSupported()` checks `isSupportedType(typeName)` regardless of null status. A null TIMESTAMP returns `false`.
3. `DecimalVector` constructed with `ArrowType.Decimal(precision, scale, 128)` — 128-bit width is standard.
4. Per-call `RootAllocator` — accepted overhead, lifecycle management deferred to future PR.
