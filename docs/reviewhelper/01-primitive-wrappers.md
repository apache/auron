# Commit 1: Primitive Arrow ColumnVector Wrappers

**Commit**: `[AURON #1851] Add primitive Arrow ColumnVector wrappers`

## What This Commit Does

Adds 7 thin wrapper classes that implement Flink's primitive `ColumnVector` sub-interfaces by delegating to Arrow `FieldVector` types. Each wrapper is ~63 lines of straightforward delegation code.

## Files to Review (7)

All in `auron-flink-runtime/src/main/java/org/apache/auron/flink/arrow/vectors/`:

| File | Arrow Vector | Flink Interface | Key Method |
|------|-------------|-----------------|------------|
| `ArrowBooleanColumnVector` | `BitVector` | `BooleanColumnVector` | `getBoolean(i)` → `vector.get(i) != 0` |
| `ArrowTinyIntColumnVector` | `TinyIntVector` | `ByteColumnVector` | `getByte(i)` → `vector.get(i)` |
| `ArrowSmallIntColumnVector` | `SmallIntVector` | `ShortColumnVector` | `getShort(i)` → `vector.get(i)` |
| `ArrowIntColumnVector` | `IntVector` | `IntColumnVector` | `getInt(i)` → `vector.get(i)` |
| `ArrowBigIntColumnVector` | `BigIntVector` | `LongColumnVector` | `getLong(i)` → `vector.get(i)` |
| `ArrowFloatColumnVector` | `Float4Vector` | `FloatColumnVector` | `getFloat(i)` → `vector.get(i)` |
| `ArrowDoubleColumnVector` | `Float8Vector` | `DoubleColumnVector` | `getDouble(i)` → `vector.get(i)` |

## What to Look For

- Each class is `public final`, implements one Flink interface
- Constructor validates with `Preconditions.checkNotNull`
- `isNullAt(i)` delegates to `vector.isNull(i)`
- `setVector()` is package-private — used by `FlinkArrowReader.reset()` (commit 5)
- `BitVector.get(i)` returns `int` (0 or 1), converted via `!= 0`
