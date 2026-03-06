# Commit 4: Nested Type Wrappers

**Commit**: `[AURON #1851] Add nested type Arrow ColumnVector wrappers`

## What This Commit Does

Adds 3 wrappers for complex/nested types (Array, Map, Row). These recursively compose child `ColumnVector` wrappers, enabling arbitrarily nested schemas.

## Files to Review (3)

| File | Arrow Vector | Flink Interface | Pattern |
|------|-------------|-----------------|---------|
| `ArrowArrayColumnVector` | `ListVector` | `ArrayColumnVector` | Offset buffer + child element vector |
| `ArrowMapColumnVector` | `MapVector` | `MapColumnVector` | Offset buffer + key/value child vectors |
| `ArrowRowColumnVector` | `StructVector` | `RowColumnVector` | VectorizedColumnBatch + reusable ColumnarRowData |

## What to Look For

- **Offset calculation** (Array + Map):
  ```
  offset = vector.getOffsetBuffer().getInt((long) i * OFFSET_WIDTH)
  length = vector.getOffsetBuffer().getInt((long) (i + 1) * OFFSET_WIDTH) - offset
  ```
  Standard Arrow list offset protocol. `OFFSET_WIDTH = 4` (inherited from `BaseRepeatedValueVector`).

- **Map data model**: `ColumnarMapData(keyColumnVector, valueColumnVector, offset, length)` — takes ColumnVectors directly, not ArrayData.

- **Row reuse**: `getRow(i)` returns the same `ColumnarRowData` instance with `setRowId(i)`. Standard Flink pattern — callers must consume before next call.

- **Constructor parameters**: Array takes `(ListVector, ColumnVector)`, Map takes `(MapVector, ColumnVector, ColumnVector)`, Row takes `(StructVector, ColumnVector[])`. Child vectors are created by `FlinkArrowReader` (commit 5).
