# Design: Arrow to Flink RowData Conversion (AURON #1851)

**Issue**: https://github.com/apache/auron/issues/1851
**AIP**: AIP-1 — Introduce Flink integration of native engine
**Author**: @weiqingy
**Status**: Draft

## 1. Motivation

Per AIP-1, the Flink integration data path is:

```
Flink RowData → Arrow (Writer) → C Data Interface / JNI → Rust/DataFusion → Arrow → Flink RowData (Reader)
                  ^^^^^^^^^^^^                                                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                  #1850 (x-tong)                                                #1851 (this issue)
```

The writer side (#1850, PR #1930) converts Flink `RowData` into Arrow `VectorSchemaRoot` for export to the native engine. This issue implements the reverse: converting Arrow vectors returned by the native engine back into Flink `RowData` so downstream Flink operators can process the results.

Without this component, the native execution results cannot flow back into the Flink pipeline.

## 2. Design Approach

### Two Candidate Approaches

**Approach A — Columnar ColumnVector wrappers (recommended)**

Follow Flink's own pattern from `flink-python`. Create thin `ColumnVector` wrappers around Arrow `FieldVector` types, then compose them into an `ArrowReader` that returns `ColumnarRowData`. This is how Flink itself reads Arrow data internally.

- Each wrapper implements a Flink `ColumnVector` sub-interface (e.g., `IntColumnVector`, `BytesColumnVector`)
- The wrapper delegates reads directly to the underlying Arrow vector — zero data copying
- `ColumnarRowData` provides a row view over the batch without materializing individual rows

**Approach B — Row-at-a-time FlinkArrowFieldReader**

Mirror the writer pattern (PR #1930). Create `FlinkArrowFieldReader` subclasses that read field-by-field into `GenericRowData`, iterating row-by-row over the batch.

- Conceptually simpler, symmetric with the writer
- Copies data element-by-element — O(rows × columns) allocations per batch

### Decision: Approach A

Approach A is chosen for these reasons:

1. **Performance**: Zero-copy columnar access. `ColumnarRowData` is just a view — no per-row object allocation. This aligns with AIP-1's goal of native vectorized execution.
2. **Flink precedent**: This is exactly how Flink's own `flink-python` module reads Arrow data. The pattern is proven and maintained upstream.
3. **Compatibility**: `ColumnarRowData` implements `RowData`, so it is transparent to all downstream Flink operators.
4. **Memory efficiency**: Arrow vectors remain the single source of truth. No duplicate data structures.

The tradeoff is more classes (one wrapper per vector type), but each is ~20 lines of delegation code.

## 3. Detailed Design

### 3.1 Package Structure

All classes in `org.apache.auron.flink.arrow` (same package as `FlinkArrowUtils`, `FlinkArrowWriter`).

```
auron-flink-runtime/src/main/java/org/apache/auron/flink/arrow/
├── FlinkArrowUtils.java          (existing)
├── FlinkArrowWriter.java         (PR #1930)
├── FlinkArrowFieldWriter.java    (PR #1930)
├── FlinkArrowReader.java         (NEW — orchestrator)
└── vectors/                      (NEW — ColumnVector wrappers)
    ├── ArrowBooleanColumnVector.java
    ├── ArrowTinyIntColumnVector.java
    ├── ArrowSmallIntColumnVector.java
    ├── ArrowIntColumnVector.java
    ├── ArrowBigIntColumnVector.java
    ├── ArrowFloatColumnVector.java
    ├── ArrowDoubleColumnVector.java
    ├── ArrowDecimalColumnVector.java
    ├── ArrowVarCharColumnVector.java
    ├── ArrowVarBinaryColumnVector.java
    ├── ArrowDateColumnVector.java
    ├── ArrowTimeColumnVector.java
    ├── ArrowTimestampColumnVector.java
    ├── ArrowArrayColumnVector.java
    ├── ArrowMapColumnVector.java
    └── ArrowRowColumnVector.java
```

### 3.2 ColumnVector Wrappers

Each wrapper implements the corresponding Flink `ColumnVector` sub-interface and delegates to the Arrow `FieldVector`. Example pattern:

```java
/**
 * Arrow-backed column vector for INT columns.
 * Wraps an Arrow IntVector and implements Flink's IntColumnVector.
 */
public final class ArrowIntColumnVector implements IntColumnVector {

    private final IntVector vector;

    public ArrowIntColumnVector(IntVector vector) {
        this.vector = Preconditions.checkNotNull(vector);
    }

    @Override
    public int getInt(int i) {
        return vector.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }
}
```

All wrappers follow this same pattern. Notable type-specific handling:

| Flink ColumnVector Interface | Arrow Vector | Notes |
|---|---|---|
| `BooleanColumnVector` | `BitVector` | |
| `ByteColumnVector` | `TinyIntVector` | |
| `ShortColumnVector` | `SmallIntVector` | |
| `IntColumnVector` | `IntVector` | Also used for `DateDayVector` (date as epoch days) |
| `LongColumnVector` | `BigIntVector` | |
| `FloatColumnVector` | `Float4Vector` | |
| `DoubleColumnVector` | `Float8Vector` | |
| `DecimalColumnVector` | `DecimalVector` | `fromUnscaledLong` for precision ≤ 18, `fromBigDecimal` otherwise |
| `BytesColumnVector` | `VarCharVector` | `new Bytes(vector.get(i))` — matches Flink upstream pattern |
| `BytesColumnVector` | `VarBinaryVector` | `new Bytes(vector.get(i))` — matches Flink upstream pattern |
| `IntColumnVector` | `DateDayVector` | Date stored as int (epoch days), same as Flink internal |
| `IntColumnVector` | `TimeMicroVector` | Convert microseconds back to milliseconds (int) to match Flink's internal representation |
| `TimestampColumnVector` | `TimeStampMicroVector` | Convert micros → `TimestampData.fromEpochMillis(millis, nanoOfMillisecond)` |
| `TimestampColumnVector` | `TimeStampMicroTZVector` | Same conversion, for `TIMESTAMP WITH LOCAL TIME ZONE` |
| `ArrayColumnVector` | `ListVector` | Recursively wraps element vector |
| `MapColumnVector` | `MapVector` | Recursively wraps key/value vectors |
| `RowColumnVector` | `StructVector` | Recursively wraps child field vectors |

### 3.3 Timestamp Precision Handling

The writer (PR #1930) normalizes all timestamps to **microsecond** precision in Arrow. The reader must reverse this:

```
Writer path:  TimestampData → microseconds (long) stored in TimeStampMicroVector
Reader path:  TimeStampMicroVector → microseconds (long) → TimestampData

Conversion:
  long micros = vector.get(i);
  long millis = micros / 1000;
  int nanoOfMillisecond = (int) (micros % 1000) * 1000;
  return TimestampData.fromEpochMillis(millis, nanoOfMillisecond);
```

Similarly for Time:
```
Writer path:  int millis → micros (long) stored in TimeMicroVector
Reader path:  TimeMicroVector → micros (long) → millis (int)

Conversion:
  return (int) (vector.get(i) / 1000);
```

This matches the writer's conversions in `FlinkArrowFieldWriter.TimestampWriter` and `FlinkArrowFieldWriter.TimeWriter`.

### 3.4 FlinkArrowReader (Orchestrator)

```java
/**
 * Reads Arrow VectorSchemaRoot data as Flink RowData.
 *
 * <p>Uses Flink's columnar data structures (ColumnarRowData backed by
 * VectorizedColumnBatch) for zero-copy access to Arrow vectors.
 */
public class FlinkArrowReader {

    private final ColumnVector[] columnVectors;
    private final ColumnarRowData reusableRow;

    private FlinkArrowReader(ColumnVector[] columnVectors) {
        this.columnVectors = columnVectors;
        VectorizedColumnBatch batch = new VectorizedColumnBatch(columnVectors);
        this.reusableRow = new ColumnarRowData(batch);
    }

    /**
     * Creates a FlinkArrowReader from a VectorSchemaRoot and RowType.
     */
    public static FlinkArrowReader create(VectorSchemaRoot root, RowType rowType) {
        List<FieldVector> fieldVectors = root.getFieldVectors();
        List<RowType.RowField> fields = rowType.getFields();
        // validate sizes match
        ColumnVector[] columns = new ColumnVector[fieldVectors.size()];
        for (int i = 0; i < fieldVectors.size(); i++) {
            columns[i] = createColumnVector(fieldVectors.get(i), fields.get(i).getType());
        }
        return new FlinkArrowReader(columns);
    }

    /**
     * Reads a row at the given position. Returns a reused RowData object
     * (callers must not hold references across calls).
     */
    public RowData read(int rowId) {
        reusableRow.setRowId(rowId);
        return reusableRow;
    }

    /**
     * Returns the number of rows in the current batch.
     */
    public int getRowCount() {
        // Delegate to the first vector's value count, or 0 if empty
    }

    // Factory dispatching per LogicalType → ColumnVector wrapper
    private static ColumnVector createColumnVector(FieldVector vector, LogicalType type) {
        // instanceof dispatch matching FlinkArrowUtils.toArrowType
    }
}
```

**Object reuse**: `read(int)` returns the same `ColumnarRowData` instance with a different `rowId`. This is standard Flink practice for columnar readers — callers must consume or copy before the next call. This matches Flink's own `ArrowReader` design.

### 3.5 Type Coverage

Must match the types supported by `FlinkArrowUtils.toArrowType()` (merged in #1959) and `FlinkArrowFieldWriter` (PR #1930):

| Flink LogicalType | Arrow Vector Type | Reader Wrapper |
|---|---|---|
| `BOOLEAN` | `BitVector` | `ArrowBooleanColumnVector` |
| `TINYINT` | `TinyIntVector` | `ArrowTinyIntColumnVector` |
| `SMALLINT` | `SmallIntVector` | `ArrowSmallIntColumnVector` |
| `INTEGER` | `IntVector` | `ArrowIntColumnVector` |
| `BIGINT` | `BigIntVector` | `ArrowBigIntColumnVector` |
| `FLOAT` | `Float4Vector` | `ArrowFloatColumnVector` |
| `DOUBLE` | `Float8Vector` | `ArrowDoubleColumnVector` |
| `DECIMAL(p, s)` | `DecimalVector` | `ArrowDecimalColumnVector` |
| `VARCHAR` / `CHAR` | `VarCharVector` | `ArrowVarCharColumnVector` |
| `VARBINARY` / `BINARY` | `VarBinaryVector` | `ArrowVarBinaryColumnVector` |
| `DATE` | `DateDayVector` | `ArrowDateColumnVector` |
| `TIME(p)` | `TimeMicroVector` | `ArrowTimeColumnVector` |
| `TIMESTAMP(p)` | `TimeStampMicroVector` | `ArrowTimestampColumnVector` |
| `TIMESTAMP_LTZ(p)` | `TimeStampMicroTZVector` | `ArrowTimestampColumnVector` (shared) |
| `ARRAY` | `ListVector` | `ArrowArrayColumnVector` |
| `MAP` | `MapVector` | `ArrowMapColumnVector` |
| `ROW` | `StructVector` | `ArrowRowColumnVector` |

### 3.6 Relationship to FlinkArrowFFIExporter

`FlinkArrowFFIExporter` (PR #1930) handles the **export** path (Flink → native). The import path (native → Flink) will eventually need a `FlinkArrowFFIImporter` that:

1. Receives Arrow arrays from native via C Data Interface
2. Imports them into a `VectorSchemaRoot`
3. Wraps with `FlinkArrowReader` to produce `RowData`

That importer is out of scope for this issue. This issue delivers the `FlinkArrowReader` layer only. The FFI import will be a separate follow-up that depends on both the reader (#1851) and the planner rule.

## 4. Dependencies

### Build Dependencies (already available in auron-flink-runtime)

These were added by PR #1930:
- `arrow-vector` — Arrow vector types
- `arrow-memory-unsafe` — Arrow memory allocator
- `flink-table-common` — Flink ColumnVector interfaces, ColumnarRowData, VectorizedColumnBatch

No additional Maven dependencies needed.

### Code Dependencies

- `FlinkArrowUtils.toArrowSchema()` / `toArrowType()` (merged in #1959) — for consistent type mapping
- Flink's `ColumnarRowData`, `VectorizedColumnBatch` from `flink-table-common`
- Flink's `ColumnVector` sub-interfaces (`IntColumnVector`, `DecimalColumnVector`, etc.)

### Blocked By

- None — can proceed independently of #1850 (writer PR). The reader reads Arrow vectors regardless of how they were produced.

### Blocks

- FFI importer (future) — needs reader to materialize RowData from imported Arrow batches
- Planner rule for `StreamExecCalc` — needs both writer + reader + FFI to complete the round-trip

## 5. Test Plan

### 5.1 Unit Tests per ColumnVector Wrapper

For each `ArrowXxxColumnVector`:
- Write known values into an Arrow vector manually
- Read back via the wrapper, assert values match
- Test null handling (set some positions null, verify `isNullAt()`)
- Test edge cases (empty vectors, max/min values, zero-length strings)

### 5.2 FlinkArrowReader Integration Tests

- Construct a `VectorSchemaRoot` with multiple column types
- Populate via Arrow API directly
- Create `FlinkArrowReader`, iterate all rows, verify values
- Test schema with all supported types in a single batch
- Test empty batch (0 rows)

### 5.3 Round-Trip Tests (Write → Read)

Once #1850 (writer) is merged:
- Create `RowData` instances with known values
- Write via `FlinkArrowWriter` into `VectorSchemaRoot`
- Read back via `FlinkArrowReader`
- Assert input and output RowData are equivalent
- Cover all supported types including nested (ARRAY, MAP, ROW)

These round-trip tests validate that the writer and reader are perfectly symmetric, which is the critical correctness property for the end-to-end pipeline.

### 5.4 Test File Location

`auron-flink-extension/auron-flink-runtime/src/test/java/org/apache/auron/flink/arrow/`
- `FlinkArrowReaderTest.java` — unit + integration tests
- `FlinkArrowRoundTripTest.java` — round-trip tests (added after #1850 merges)

### Build & Run

```bash
# Build
./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 -Pflink,flink-1.18 -DskipBuildNative

# Run reader tests
./build/mvn test -pl auron-flink-extension/auron-flink-runtime -am \
  -Pscala-2.12 -Pflink-1.18 -Pspark-3.5 -Prelease \
  -Dtest=FlinkArrowReaderTest
```

## 6. Implementation Plan

The work is split into two PRs for reviewability:

### PR 1: ColumnVector wrappers + FlinkArrowReader + unit tests

Files:
- 16 `ArrowXxxColumnVector` classes in `vectors/` subpackage
- `FlinkArrowReader.java`
- `FlinkArrowReaderTest.java`

### PR 2 (after #1850 merges): Round-trip tests

Files:
- `FlinkArrowRoundTripTest.java`

This split allows PR 1 to proceed immediately without waiting for the writer.

## 7. Review Feedback & Responses

### 7.1 String/VarChar Allocation (raised as critical)

**Claim**: `VarCharVector.get(i)` allocates a new `byte[]` per row, defeating zero-copy.

**Assessment**: Partially valid, but matches Flink's own approach. Flink's upstream `ArrowVarCharColumnVector` in `flink-python` does exactly `new Bytes(varCharVector.get(i), 0, bytes.length)`. The `BytesColumnVector` interface returns a `Bytes` object (which holds `byte[] data`, `int offset`, `int len`), so some allocation is inherent to the interface contract.

True zero-copy would require bypassing `BytesColumnVector` entirely and accessing Arrow's `dataBuffer`/`offsetBuffer` via `MemorySegment` — but this would break the `ColumnarRowData` abstraction and is not how Flink's internal readers work.

**Decision**: Match Flink's upstream implementation. The per-row `byte[]` allocation is bounded by the batch size (typically ~10K rows), and string processing is already dominated by downstream serialization costs. This is a valid optimization target for a future PR if profiling shows it as a bottleneck, but not a correctness issue and not a blocker for the initial implementation.

### 7.2 Decimal Allocation Optimization (raised as critical)

**Claim**: `DecimalVector.getObject(i)` allocates a `BigDecimal` per row; should use `fromUnscaledLong` for precision ≤ 18.

**Assessment**: Valid optimization. Flink's own implementation also uses `fromBigDecimal(vector.getObject(i))`, so matching upstream is correct. However, Flink's `DecimalData` uses a compact `long` representation for precision ≤ 18. We can avoid the `BigDecimal` intermediate by reading the raw 128-bit value from Arrow and extracting the `long` directly.

**Decision**: Implement with the optimization:
```java
@Override
public DecimalData getDecimal(int i, int precision, int scale) {
    if (precision <= DecimalData.MAX_COMPACT_PRECISION) {
        // Read raw bytes from Arrow's 128-bit storage, extract as long
        byte[] bytes = decimalVector.get(i);
        long unscaledLong = extractUnscaledLong(bytes);
        return DecimalData.fromUnscaledLong(unscaledLong, precision, scale);
    }
    return DecimalData.fromBigDecimal(decimalVector.getObject(i), precision, scale);
}
```
This avoids `BigDecimal` allocation for the common case (precision ≤ 18) while falling back to the safe path for large decimals.

### 7.3 Dictionary Encoding Support (raised as architectural risk)

**Claim**: DataFusion may return `DictionaryVector` for string columns, causing `ClassCastException`.

**Assessment**: Not applicable for Phase 1. The Calc operator evaluates scalar expressions (projections, filters) — DataFusion processes the Arrow batch and returns vectors of the same types that were input. Dictionary encoding is an optimization used in scan/shuffle operations, not in expression evaluation output. The writer sends `VarCharVector`; the native engine will return `VarCharVector`.

**Decision**: Out of scope for this PR. If a future operator (e.g., scan pushdown) introduces dictionary-encoded vectors, a `DictionaryVector` wrapper can be added then. The `createColumnVector` factory is extensible — adding a new branch for `DictionaryVector` is straightforward.

### 7.4 Memory Management / AutoCloseable (raised as architectural risk)

**Claim**: Arrow off-heap memory must be freed; `FlinkArrowReader` should implement `AutoCloseable`.

**Assessment**: Valid. The reader is a view and does not own the `VectorSchemaRoot`, but implementing `AutoCloseable` makes ownership semantics explicit and enables use in try-with-resources. The `close()` method should document that it does NOT close the underlying root — the caller (FFI importer) manages that lifecycle.

**Decision**: Accepted. `FlinkArrowReader` will implement `AutoCloseable` with a no-op `close()` and clear documentation:
```java
/**
 * Implements AutoCloseable for use in resource management blocks.
 * Note: this does NOT close the underlying VectorSchemaRoot.
 * The caller that created the root is responsible for its lifecycle.
 */
@Override
public void close() {
    // Reader is a view; root lifecycle managed by caller
}
```

### 7.5 Timestamp Timezone Handling (raised as minor)

**Claim**: Should respect Flink's session timezone when converting `TIMESTAMP_LTZ`.

**Assessment**: Not applicable at this layer. The writer normalizes all timestamps to UTC microseconds. The reader reverses the conversion — `micros → TimestampData.fromEpochMillis(millis, nanoOfMillisecond)`. `TimestampData` is timezone-agnostic (it stores epoch-based values). Session timezone handling is the responsibility of Flink's SQL layer when presenting results to users, not the Arrow serialization layer.

**Decision**: No change needed.

### 7.6 Nested Type Nullability (raised as minor)

**Claim**: Must check `isNullAt` on parent before reading children in nested types.

**Assessment**: Already handled by the `ColumnarRowData` contract. When the caller checks `isNullAt(i)` on the row (which delegates to the parent ColumnVector), it returns true, and the caller should not call `getArray`/`getMap`/`getRow`. This is the standard Flink contract for `RowData`. However, the wrapper implementations should be defensive — if the parent is null, the Arrow child vectors may contain garbage at those positions.

**Decision**: Document the contract. The wrappers for `Array`, `Map`, and `Row` will follow Flink's upstream pattern where `isNullAt` delegates to the parent vector. Callers are expected to check nullability before accessing nested values per the `RowData` contract.

### 7.7 Reader Reusability Across Batches (raised as minor)

**Claim**: Allow swapping `VectorSchemaRoot` without reinstantiating wrappers.

**Assessment**: Valid and useful. In a streaming pipeline, the same operator processes many batches with the same schema. Reinstantiating 16+ wrappers per batch is wasteful.

**Decision**: Accepted. Add a `reset(VectorSchemaRoot newRoot)` method that swaps the underlying vectors in each wrapper without reallocating:
```java
/**
 * Resets the reader to use a new VectorSchemaRoot with the same schema.
 * Avoids reinstantiating column vector wrappers for each batch.
 */
public void reset(VectorSchemaRoot newRoot) {
    List<FieldVector> newVectors = newRoot.getFieldVectors();
    for (int i = 0; i < columnVectors.length; i++) {
        ((ResettableColumnVector) columnVectors[i]).reset(newVectors.get(i));
    }
}
```
Each wrapper will implement a package-private `ResettableColumnVector` interface with a `reset(FieldVector)` method. This is a minor addition to each wrapper class.

## 8. Alternatives Considered

### 8.1 Approach B: Row-at-a-time with FlinkArrowFieldReader

Mirror the writer's `FlinkArrowFieldWriter` pattern with reader counterparts that extract field values into `GenericRowData`.

**Rejected because**:
- Allocates one `GenericRowData` per row (or requires manual object reuse)
- Copies every value from Arrow vector into Java objects
- Flink's own Arrow integration uses the columnar approach for good reason
- Does not leverage the vectorized execution model that AIP-1 is built on

### 8.2 Using Flink's built-in Arrow readers directly

Flink's `flink-python` module contains Arrow column vector wrappers. We could reuse them.

**Rejected because**:
- They live in `flink-python` which is a large dependency with Python-specific code
- They are annotated `@Internal` — not part of Flink's public API
- The wrappers are simple enough that reimplementing them avoids the coupling
- Auron's timestamp precision handling (always microseconds) may differ from Flink's general-purpose implementation
