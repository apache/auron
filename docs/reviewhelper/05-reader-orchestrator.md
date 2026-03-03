# Commit 5: FlinkArrowReader Orchestrator

**Commit**: `[AURON #1851] Add FlinkArrowReader orchestrator`

## What This Commit Does

Adds the central `FlinkArrowReader` class that ties all 16 wrappers together. It creates the appropriate `ColumnVector` wrapper for each field based on the Flink `LogicalType`, and provides the public API for reading rows.

## File to Review (1)

`auron-flink-runtime/src/main/java/org/apache/auron/flink/arrow/FlinkArrowReader.java`

## Public API

| Method | Purpose |
|--------|---------|
| `create(VectorSchemaRoot, RowType)` | Factory — creates wrappers, validates field counts |
| `read(int rowId)` | Returns reusable `ColumnarRowData` at given row position |
| `getRowCount()` | Delegates to `root.getRowCount()` |
| `reset(VectorSchemaRoot)` | Swaps to new batch with same schema |
| `close()` | No-op — reader is a view, caller manages root lifecycle |

## What to Look For

- **`createColumnVector` switch** (key review point): Dispatches `LogicalTypeRoot` → correct wrapper + Arrow vector cast. Verify every type from `FlinkArrowUtils.toArrowType()` is covered.
- **TIME/TIMESTAMP comments**: Documents that the writer normalizes to microseconds, so the reader always expects `TimeMicroVector` / `TimeStampVector`.
- **Nested type factories**: `createArrayColumnVector`, `createMapColumnVector`, `createRowColumnVector` recursively call `createColumnVector` for child types.
- **Null checks**: `Preconditions.checkNotNull` on `create()` and `reset()` params. `reset()` also validates field count with `checkArgument`.
- **`reset()` approach**: Recreates all column vectors using stored `RowType` rather than per-type vector swapping. Simpler, runs once per batch (not per row).
