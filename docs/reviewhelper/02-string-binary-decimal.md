# Commit 2: String, Binary, and Decimal Wrappers

**Commit**: `[AURON #1851] Add string, binary, and decimal Arrow ColumnVector wrappers`

## What This Commit Does

Adds 3 wrappers for variable-length and decimal types. The decimal wrapper includes an optimization to avoid `BigDecimal` allocation for common precision values.

## Files to Review (3)

| File | Arrow Vector | Flink Interface | Notes |
|------|-------------|-----------------|-------|
| `ArrowVarCharColumnVector` | `VarCharVector` | `BytesColumnVector` | `getBytes(i)` returns `new Bytes(vector.get(i), 0, len)` |
| `ArrowVarBinaryColumnVector` | `VarBinaryVector` | `BytesColumnVector` | Same pattern as VarChar |
| `ArrowDecimalColumnVector` | `DecimalVector` | `DecimalColumnVector` | Compact + wide path |

## What to Look For

- **VarChar/VarBinary**: `vector.get(i)` returns `byte[]` (allocation per call — inherent to Arrow API, matches Flink upstream)
- **Decimal optimization** (key review point):
  - Precision ≤ 18: reads 8 bytes directly from `vector.getDataBuffer()` via `ArrowBuf.getLong(offset)`, avoids `BigDecimal`
  - Precision > 18: falls back to `DecimalData.fromBigDecimal(vector.getObject(i), ...)`
  - `MAX_COMPACT_PRECISION = 18` is a local constant because Flink's `DecimalData.MAX_COMPACT_PRECISION` is package-private
