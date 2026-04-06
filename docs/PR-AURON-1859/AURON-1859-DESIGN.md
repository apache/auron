# Design — AURON-1859: Convert Math Operators to Auron Native Operators

**Rev 1** — 2026-04-01
**Issue**: https://github.com/apache/auron/issues/1859
**Prerequisite**: #1856 (Flink Node Converter Tools) — PR #2146 merged

---

## 1. Problem Statement

The Flink integration track needs concrete expression converters that translate Flink/Calcite `RexNode` expressions into Auron native `PhysicalExprNode` protobuf representations. Issue #1859 targets math operators (`+`, `-`, `*`, `/`, `%`) as the first converters, unlocking #1857 (FlinkAuronCalcOperator) and #1853 (StreamExecCalc rewrite).

The converter framework from #1856 provides the dispatch infrastructure (`FlinkNodeConverterFactory`, `FlinkRexNodeConverter`, `ConverterContext`) but has zero concrete converter implementations. This PR delivers the first three.

---

## 2. Approach Candidates

### Candidate A: Single RexCallConverter with SqlKind Switch

One `RexCallConverter` registered for `RexCall.class`, dispatching by `SqlKind` internally.

**Pros**: Matches the factory's class-based dispatch design. Simple, one registration call.
**Cons**: Class will grow as more operators are added in future PRs (#1860, #1861, #1864).

### Candidate B: Per-Operator Converter Classes (Gluten-style)

Separate converter class for each operator (e.g., `PlusConverter`, `MinusConverter`).

**Pros**: Small focused classes.
**Cons**: Cannot work with our factory — it keys by `RexNode` subclass (`RexCall.class`), so only ONE converter can be registered for `RexCall`. Would require redesigning the factory.

**Evidence**: The factory throws `IllegalArgumentException` on duplicate registration:
```java
// FlinkNodeConverterFactory.java:77-79
if (rexConverterMap.containsKey(nodeClass)) {
    throw new IllegalArgumentException("Duplicate RexNode converter for " + nodeClass.getName());
}
```

### Candidate C: Sub-dispatch Registry Inside RexCallConverter (Gluten hybrid)

One `RexCallConverter` for the factory, but internally uses a `Map<SqlKind, BiFunction>` to delegate to handler methods. Future PRs add entries to this map.

**Pros**: Extensible without modifying the switch statement. Separates concerns.
**Cons**: More infrastructure than needed for 7 operators. Over-engineered for Phase 1.

### Decision: **Candidate A** — Single RexCallConverter with SqlKind Switch

**Rationale**:
- Matches Spark Auron's pattern (single `convertExprWithFallback()` with pattern match — `NativeConverters.scala:395-1183`)
- The factory's class-based dispatch forces one converter per RexNode subclass anyway
- Simple switch statement is readable and maintainable up to ~20 cases
- Future PRs (#1860, #1861, #1864) just add new `case` clauses — additive, no structural changes
- If it grows beyond ~30 cases, we can refactor to Candidate C later

---

## 3. Detailed Design

### 3.1 Converter Classes

Three new Java classes in `org.apache.auron.flink.table.planner.converter`:

#### 3.1.1 RexInputRefConverter

Converts column references to native `PhysicalColumn` protobuf.

**Prior art**:
- **Gluten-Flink**: `RexNodeConverter.toTypedExpr()` handles `RexInputRef` by resolving the column name from `context.getInputAttributeNames()` using the index, then creates `FieldAccessTypedExpr(type, name)` — Source: `gluten-flink/planner/src/main/java/org/apache/gluten/rexnode/RexNodeConverter.java`
- **Spark Auron**: Column references are converted elsewhere (in plan-level conversion, not expression-level) — Source: `NativeConverters.scala:1319-1325`

**Design**:
```java
public class RexInputRefConverter implements FlinkRexNodeConverter {

    @Override
    public Class<? extends RexNode> getNodeClass() {
        return RexInputRef.class;
    }

    @Override
    public boolean isSupported(RexNode node, ConverterContext context) {
        RexInputRef ref = (RexInputRef) node;
        return ref.getIndex() < context.getInputType().getFieldCount();
    }

    @Override
    public PhysicalExprNode convert(RexNode node, ConverterContext context) {
        RexInputRef ref = (RexInputRef) node;
        int index = ref.getIndex();
        String name = context.getInputType().getFieldNames().get(index);
        return PhysicalExprNode.newBuilder()
                .setColumn(PhysicalColumn.newBuilder()
                        .setName(name)
                        .setIndex(index)
                        .build())
                .build();
    }
}
```

**Target protobuf**: `PhysicalColumn` (name + index) — Source: `auron.proto:512-515`

#### 3.1.2 RexLiteralConverter

Converts scalar literal values to native `ScalarValue` protobuf using Arrow IPC serialization.

**Prior art**:
- **Spark Auron**: Serializes literals as Arrow IPC bytes via `ArrowStreamWriter` → `ScalarValue.ipc_bytes` — Source: `NativeConverters.scala:409-430`
- **Gluten-Flink**: Uses individually typed `Variant` values (`IntegerValue`, `BigIntValue`, etc.) via `toVariant()` — Source: `gluten-flink/planner/src/main/java/org/apache/gluten/rexnode/RexNodeConverter.java`
- **Native engine (Rust)**: Deserializes `ScalarValue.ipc_bytes` via Arrow IPC `StreamReader` → `ScalarValue::try_from_array` — Source: `auron-planner/src/lib.rs:446-455`

**Decision: Arrow IPC bytes (Spark pattern)**

Auron's `ScalarValue` protobuf has only `ipc_bytes` (no typed fields) — Source: `auron.proto:873-875`. We must use Arrow IPC serialization. Gluten's approach (typed Variant objects) targets Velox, not DataFusion.

**Design**:
```java
public class RexLiteralConverter implements FlinkRexNodeConverter {

    @Override
    public Class<? extends RexNode> getNodeClass() {
        return RexLiteral.class;
    }

    @Override
    public boolean isSupported(RexNode node, ConverterContext context) {
        RexLiteral literal = (RexLiteral) node;
        if (literal.isNull()) {
            return true;  // null literals always supported
        }
        switch (literal.getType().getSqlTypeName()) {
            case TINYINT: case SMALLINT: case INTEGER: case BIGINT:
            case FLOAT: case DOUBLE: case DECIMAL:
            case BOOLEAN: case CHAR: case VARCHAR:
                return true;
            default:
                return false;
        }
    }

    @Override
    public PhysicalExprNode convert(RexNode node, ConverterContext context) {
        RexLiteral literal = (RexLiteral) node;
        byte[] ipcBytes = serializeToArrowIpc(literal);
        return PhysicalExprNode.newBuilder()
                .setLiteral(ScalarValue.newBuilder()
                        .setIpcBytes(ByteString.copyFrom(ipcBytes))
                        .build())
                .build();
    }

    // Creates a single-row Arrow IPC stream from the literal value
    private byte[] serializeToArrowIpc(RexLiteral literal) {
        // 1. Map SqlTypeName to Arrow Field
        // 2. Create VectorSchemaRoot with one column
        // 3. Set value into the appropriate vector (or leave null)
        // 4. Serialize via ArrowStreamWriter to ByteArrayOutputStream
        // 5. Return bytes
    }
}
```

**Type mapping for value extraction**:

| SqlTypeName | Arrow Vector | RexLiteral extraction | Evidence |
|---|---|---|---|
| `TINYINT` | `TinyIntVector` | `getValueAs(Byte.class)` | Gluten: `Integer.valueOf(literal.getValue().toString())` |
| `SMALLINT` | `SmallIntVector` | `getValueAs(Short.class)` | Gluten: same pattern |
| `INTEGER` | `IntVector` | `getValueAs(Integer.class)` | Gluten: same pattern |
| `BIGINT` | `BigIntVector` | `getValueAs(Long.class)` | Gluten: `Long.valueOf(literal.getValue().toString())` |
| `FLOAT` | `Float4Vector` | `getValueAs(Float.class)` | (Gluten lacks FLOAT — gap in their impl) |
| `DOUBLE` | `Float8Vector` | `getValueAs(Double.class)` | Gluten: `Double.valueOf(literal.getValue().toString())` |
| `DECIMAL` | `DecimalVector` | `getValueAs(BigDecimal.class)` | Gluten: `literal.getValueAs(BigDecimal.class)` |
| `BOOLEAN` | `BitVector` | `getValueAs(Boolean.class)` | Gluten: `(boolean) literal.getValue()` |
| `CHAR`/`VARCHAR` | `VarCharVector` | `getValueAs(String.class)` | Gluten: `literal.getValueAs(String.class)` |
| `NULL` | (any nullable) | N/A — leave vector null | (Gluten lacks null handling — gap) |

**Arrow type construction**: Reuse `SchemaConverters.convertToAuronArrowType()` for proto ArrowType, but for Arrow Java `Field`, use the standard `ArrowType` mapping (e.g., `new ArrowType.Int(32, true)` for INT32). A helper method `sqlTypeNameToArrowField()` will map `SqlTypeName` → Arrow Java `Field`.

**Null handling**: For null literals, create the vector but don't call `setSafe()`. The validity buffer defaults to null. This is the same pattern as Spark — Source: `NativeConverters.scala:412` (`e.eval(null)` returns null for null literals).

#### 3.1.3 RexCallConverter

Converts function/operator calls to native expressions. Dispatches by `SqlKind`.

**Prior art**:
- **Gluten-Flink**: `RexCallConverterFactory` dispatches by operator name string. `BasicArithmeticOperatorRexCallConverter` handles `+`, `-`, `*`. Each converter calls `getParams()` which recursively converts operands via `RexNodeConverter.toTypedExpr()` — Source: `gluten-flink/planner/src/main/java/org/apache/gluten/rexnode/functions/BaseRexCallConverters.java`
- **Spark Auron**: Pattern match per expression class, calls `buildBinaryExprNode(lhs, rhs, opString)` which recursively calls `convertExprWithFallback()` — Source: `NativeConverters.scala:576-774`

**Design**:
```java
public class RexCallConverter implements FlinkRexNodeConverter {

    private static final Set<SqlKind> SUPPORTED_KINDS = EnumSet.of(
            SqlKind.PLUS, SqlKind.MINUS, SqlKind.TIMES,
            SqlKind.DIVIDE, SqlKind.MOD,
            SqlKind.MINUS_PREFIX,  // unary minus
            SqlKind.PLUS_PREFIX,   // unary plus (identity)
            SqlKind.CAST           // basic numeric cast
    );

    @Override
    public Class<? extends RexNode> getNodeClass() {
        return RexCall.class;
    }

    @Override
    public boolean isSupported(RexNode node, ConverterContext context) {
        RexCall call = (RexCall) node;
        if (!SUPPORTED_KINDS.contains(call.getKind())) {
            return false;
        }
        // For arithmetic ops, verify the result type is numeric.
        // This prevents accepting TIMESTAMP + INTERVAL (also SqlKind.PLUS)
        // or other non-numeric uses of arithmetic SqlKinds.
        switch (call.getKind()) {
            case PLUS: case MINUS: case TIMES: case DIVIDE: case MOD:
            case MINUS_PREFIX:
                return isNumericType(call.getType());
            case CAST:
                return isNumericType(call.getType());
            case PLUS_PREFIX:
                return true;
            default:
                return false;
        }
    }

    private static boolean isNumericType(RelDataType type) {
        switch (type.getSqlTypeName()) {
            case TINYINT: case SMALLINT: case INTEGER: case BIGINT:
            case FLOAT: case DOUBLE: case DECIMAL:
                return true;
            default:
                return false;
        }
    }

    @Override
    public PhysicalExprNode convert(RexNode node, ConverterContext context) {
        RexCall call = (RexCall) node;
        switch (call.getKind()) {
            case PLUS:         return buildBinaryExpr(call, "Plus", context);
            case MINUS:        return buildBinaryExpr(call, "Minus", context);
            case TIMES:        return buildBinaryExpr(call, "Multiply", context);
            case DIVIDE:       return buildDivide(call, context);
            case MOD:          return buildModulo(call, context);
            case MINUS_PREFIX: return buildNegative(call, context);
            case PLUS_PREFIX:  return convertOperand(call.getOperands().get(0), context);
            case CAST:         return buildCast(call, context);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported RexCall kind: " + call.getKind());
        }
    }
}
```

**Recursive operand conversion**:

Following Gluten-Flink's two-layer recursion pattern (converter calls back into factory for operands — Source: `RexNodeConverter.java` → `BaseRexCallConverters.getParams()` → `RexNodeConverter.toTypedExpr()`), the `RexCallConverter` converts operands via the factory.

**Factory injection** (not singleton access): The `RexCallConverter` takes a `FlinkNodeConverterFactory` reference in its constructor rather than calling `getInstance()`. This aligns with #1856's testability design — the factory test (`FlinkNodeConverterFactoryTest.java:53`) uses a package-private constructor to create fresh instances for test isolation. Our tests do the same: create a fresh factory, register all 3 converters, test against that instance.

```java
public class RexCallConverter implements FlinkRexNodeConverter {
    private final FlinkNodeConverterFactory factory;

    public RexCallConverter(FlinkNodeConverterFactory factory) {
        this.factory = Objects.requireNonNull(factory, "factory must not be null");
    }

    private PhysicalExprNode convertOperand(RexNode operand, ConverterContext context) {
        return factory.convertRexNode(operand, context)
                .orElseThrow(() -> new IllegalStateException(
                        "Failed to convert operand: " + operand));
    }
```

This handles nested expressions naturally: `(a + 1) * b` → the outer `TIMES` RexCall converts its left operand (a `PLUS` RexCall), which recursively converts `a` (RexInputRef) and `1` (RexLiteral).

**Binary expression builder**:

```java
private PhysicalExprNode buildBinaryExpr(
        RexCall call, String op, ConverterContext context) {
    PhysicalExprNode left = convertOperand(call.getOperands().get(0), context);
    PhysicalExprNode right = convertOperand(call.getOperands().get(1), context);
    return PhysicalExprNode.newBuilder()
            .setBinaryExpr(PhysicalBinaryExprNode.newBuilder()
                    .setL(left)
                    .setR(right)
                    .setOp(op)
                    .build())
            .build();
}
```

**Evidence**: Spark uses the same pattern — `buildBinaryExprNode(lhs, rhs, "Plus")` at `NativeConverters.scala:612`.

### 3.2 Division by Zero Handling

**Problem**: DataFusion errors on integer division by zero. Flink SQL expects NULL.

**Prior art**:
- **Spark Auron**: Wraps the divisor in `Spark_NullIfZero` extended scalar function — Source: `NativeConverters.scala:735,748,771`
- **Gluten-Flink**: Does NOT add any div-by-zero protection. Relies on Velox returning NULL natively — Source: `gluten-flink/.../BaseRexCallConverters.java`
- **Native engine naming convention** (`datafusion-ext-functions/src/lib.rs:43-44`):
  ```rust
  // auron ext functions, if used for spark should be start with 'Spark_',
  // if used for flink should be start with 'Flink_',
  // same to other engines.
  ```

**Critical finding**: The native engine's `create_auron_ext_function()` (`lib.rs:89`) has an exhaustive match that errors on unknown names:
```rust
_ => df_unimplemented_err!("spark ext function not implemented: {name}")?,
```
Calling `Flink_NullIfZero` would hit this default and **error at runtime**. Using `Spark_NullIfZero` works but violates the project's naming convention.

**Decision**: **Defer div-by-zero wrapping to #1857.**

**Rationale**:
1. Adding `Flink_NullIfZero` requires a one-line Rust change (`lib.rs`), which expands this Java-only PR into a cross-language change
2. This PR is about the **converter framework** (RexNode → protobuf translation). Division-by-zero is a **runtime behavior** concern
3. The gap is documented and will be caught immediately in #1857 integration tests
4. #1857 (FlinkAuronCalcOperator) is the natural place to add `Flink_NullIfZero` since it owns the runtime pipeline and can include the Rust registration alongside the Java wrapping

DIVIDE and MOD will be converted as straightforward `PhysicalBinaryExprNode` in this PR — same as PLUS/MINUS/TIMES. The `wrapNullIfZero` logic will be added in #1857.

**Design for DIVIDE (this PR)**:
```java
// Simple conversion — div-by-zero handling deferred to #1857
private PhysicalExprNode buildBinaryExpr(RexCall call, String op, ConverterContext context) {
    PhysicalExprNode left = convertOperand(call.getOperands().get(0), context);
    PhysicalExprNode right = convertOperand(call.getOperands().get(1), context);
    return PhysicalExprNode.newBuilder()
            .setBinaryExpr(PhysicalBinaryExprNode.newBuilder()
                    .setL(left).setR(right).setOp(op).build())
            .build();
}
```

### 3.3 CAST Handling

**Prior art**:
- **Spark Auron**: CAST is in the same `convertExprWithFallback()` as arithmetic. Uses `PhysicalTryCastNode` (not `PhysicalCastNode`) as the default — Source: `NativeConverters.scala:473-503`
- **Gluten-Flink**: CAST is registered in the same `RexCallConverterFactory` map as arithmetic via `Map.entry("CAST", Arrays.asList(() -> new DefaultRexCallConverter("cast")))` — Source: `RexCallConverterFactory.java`
- **Flink/Calcite**: Optimizer inserts explicit CAST RexCall nodes for type promotion in arithmetic (via `StandardConvertletTable.convertOperands()` → `RexBuilder.ensureType()` → `makeCast()`) — Source: `apache/calcite StandardConvertletTable.java`

**Decision**: Include basic numeric CAST in this PR. Use `PhysicalTryCastNode` (not `PhysicalCastNode`).

**Rev 2 update** (2026-04-04): Originally chose `PhysicalCastNode` for Flink's strict CAST semantics. Revised to `PhysicalTryCastNode` per reviewer @Tartarus0zm's PoC, which uses TryCast for both operand promotion casts and explicit CAST expressions. For numeric-to-numeric widening (INT→BIGINT), both behave identically. Aligning with the PoC keeps the codebase consistent with the reviewer's architectural direction.

**Design**:
```java
private PhysicalExprNode buildTryCast(RexCall call, ConverterContext context) {
    PhysicalExprNode operand = convertOperand(call.getOperands().get(0), context);
    return wrapInTryCast(operand, call.getType());
}
```

**Type conversion chain**: `RelDataType` (Calcite) → `LogicalType` (Flink) → `ArrowType` (Auron protobuf).

The `RelDataType` → `LogicalType` conversion needs a helper. Flink provides `FlinkTypeFactory.toLogicalType(RelDataType)` but this requires a `FlinkTypeFactory` instance. Alternatively, we can map `SqlTypeName` directly to `LogicalType`:

| SqlTypeName | Flink LogicalType |
|---|---|
| `TINYINT` | `new TinyIntType()` |
| `SMALLINT` | `new SmallIntType()` |
| `INTEGER` | `new IntType()` |
| `BIGINT` | `new BigIntType()` |
| `FLOAT` | `new FloatType()` |
| `DOUBLE` | `new DoubleType()` |
| `DECIMAL(p,s)` | `new DecimalType(p, s)` |
| `BOOLEAN` | `new BooleanType()` |
| `VARCHAR` | `new VarCharType()` |

This mapping is straightforward and avoids a dependency on `FlinkTypeFactory`. A utility method `toLogicalType(RelDataType)` will be added to `RexCallConverter` (or a small `TypeConversionUtils` helper if shared).

### 3.4 Type Coercion: Not Our Problem

**Evidence that Flink inserts CASTs upstream**:

Calcite's `StandardConvertletTable.convertOperands()` method calls `RexBuilder.ensureType()` which creates CAST RexCall nodes when the operand type doesn't match the validated return type. For `INT + BIGINT`, the validated return type is `BIGINT` (via `ReturnTypes.LEAST_RESTRICTIVE`), so the INT operand gets wrapped in `CAST(INT AS BIGINT)`.

**Source**: `apache/calcite core/src/main/java/org/apache/calcite/sql2rel/StandardConvertletTable.java` — `convertOperands` method.

**Result**: By the time our converter sees a `RexCall(PLUS, ...)`, operands already have matching types (or explicit CAST wrappers). Our arithmetic converter doesn't need type promotion logic.

**Gluten-Flink confirmation**: Gluten adds its own `TypeUtils.promoteTypeForArithmeticExpressions()` as a safety net. Our design is simpler — we trust Calcite's upstream CASTs and handle them via the CAST case in our converter. If edge cases surface during integration testing (#1857), we can add a safety net then.

---

## 4. Prior Art Comparison Table

| Aspect | Spark Auron | Gluten-Flink | **Auron Flink (this PR)** |
|---|---|---|---|
| **Dispatch** | Pattern match in Scala | Factory keyed by operator name | Factory keyed by RexNode class + SqlKind switch |
| **Literal format** | Arrow IPC bytes | Typed Variant values (Velox) | **Arrow IPC bytes** (same proto) |
| **Column ref** | Plan-level (not expr) | Field name from context | **PhysicalColumn(name, index)** |
| **Recursion** | Direct recursive call | `getParams()` → `toTypedExpr()` | **`convertOperand()` → factory** |
| **Div-by-zero** | `Spark_NullIfZero` wrapper | None (Velox handles) | **Deferred to #1857** (needs `Flink_NullIfZero` Rust registration) |
| **CAST** | `PhysicalTryCastNode` | `DefaultRexCallConverter("cast")` | **`PhysicalCastNode`** |
| **Type promotion** | Manual in converter | `TypeUtils.promoteTypes()` | **Trust Calcite CASTs** |
| **Registration** | None (static pattern match) | Static `Map.ofEntries(...)` | **Dynamic `registerRexConverter()`** |

---

## 5. Alignment with #1856 Framework

This design was verified against the #1856 SPEC, PLAN, and research artifacts:

| #1856 Contract | #1859 Compliance | Evidence |
|---|---|---|
| `FlinkRexNodeConverter` implements `FlinkNodeConverter<RexNode>` | All 3 converters implement `FlinkRexNodeConverter` | Design §3.1.1–3.1.3 |
| Factory dispatches by `node.getClass()` | 3 distinct classes: `RexInputRef`, `RexLiteral`, `RexCall` — no conflicts | `FlinkNodeConverterFactory.java:109` |
| `convert()` returns `PhysicalExprNode` | All converters return `PhysicalExprNode` | Per Rev 3 reviewer feedback (March 23) |
| `ConverterContext` carries `RowType inputType` | `RexInputRefConverter` uses `getInputType()` for column resolution | `ConverterContext.java:77` |
| Factory singleton with `registerRexConverter()` | Converters are standalone; registration deferred to #1857 | #1856 SPEC "Out of Scope" lists concrete converters |
| Package: `o.a.auron.flink.table.planner.converter` | All new files in same package | Consistent with existing 5 files |
| Java only | All new code is Java | #1856 research resolved Q4 |

**Factory injection for testability**: #1856's `FlinkNodeConverterFactoryTest` uses a package-private `FlinkNodeConverterFactory()` constructor (line 53) for test isolation. `RexCallConverter` follows this pattern by accepting the factory as a constructor parameter rather than calling `getInstance()`, enabling tests with fresh factory instances.

**Concrete converters as explicit #1856 follow-up**: The #1856 SPEC lists "Concrete converter implementations (RexLiteralConverter, RexInputRefConverter, RexCallConverter, etc.)" as out-of-scope, confirming #1859 is the intended follow-up.

---

## 6. Dependencies (no new deps)

### Existing (no new deps)

| Artifact | Used for | Evidence |
|---|---|---|
| `arrow-vector` (in planner pom.xml) | `ArrowStreamWriter`, vectors | `pom.xml:290-293` |
| `arrow-c-data` (in planner pom.xml) | `CDataDictionaryProvider` | `pom.xml:276-278` |
| `arrow-memory-unsafe` (in planner pom.xml) | Arrow memory allocation | `pom.xml:285-288` |
| `auron-flink-runtime` (in planner pom.xml) | `SchemaConverters`, `FlinkArrowUtils` | `pom.xml:250-253` |

### Native Engine (no changes)

All operators and functions already registered:
- Binary ops: `from_proto_binary_op()` — Source: `auron-planner/src/lib.rs:70-101`
- `Spark_NullIfZero`: exists at `datafusion-ext-functions/src/lib.rs:49` — NOT used in this PR (deferred to #1857 as `Flink_NullIfZero`)
- Expression parsing: `try_parse_physical_expr()` — Source: `auron-planner/src/planner.rs:829-1038`

---

## 7. Test Plan

| Test Class | # Tests | What it validates |
|---|---|---|
| `RexInputRefConverterTest` | 4 | Column ref conversion, getNodeClass, isSupported |
| `RexLiteralConverterTest` | 9 | Int/Long/Double/Boolean/Null/String/Decimal literals, getNodeClass, unsupported type |
| `RexCallConverterTest` | 11 | All 5 arithmetic ops, unary minus/plus, CAST, nested expr, unsupported, getNodeClass |
| **Total** | **24** | |

Tests will use `RexBuilder` (Calcite) to create RexNode instances and verify the output `PhysicalExprNode` protobuf structure. Following the pattern from `FlinkNodeConverterFactoryTest.java`.

For `RexCallConverterTest`, all three converters must be registered in the factory before testing (since operand conversion is recursive through the factory).

---

## 8. Alternatives Considered

### 7.1 Skip CAST — Let #1864 Handle It

**Rejected**. Mixed-type arithmetic won't work without CAST support. Flink's optimizer inserts CAST nodes upstream, so our converter must handle them. Including basic numeric CAST here (5 lines of code) is more pragmatic than shipping broken arithmetic.

### 7.2 Use PhysicalTryCastNode Instead of PhysicalCastNode

**Rejected for Flink**. Spark uses `TryCast` because Spark's CAST is lenient. Flink's CAST is strict by default — `PhysicalCastNode` is the correct match. `TRY_CAST` is a separate Flink function.

### 7.3 Implement Type Promotion in Converter (Like Gluten)

**Rejected**. Calcite already inserts CASTs. Adding our own promotion is redundant and risks conflicting with Calcite's decisions. If edge cases surface in integration testing (#1857), we can add a safety net then.

### 7.4 Use Spark_NullIfZero from Flink Code

**Rejected**. The native engine's naming convention (`lib.rs:43-44`) explicitly requires Flink functions to use `Flink_` prefix. Using `Spark_NullIfZero` would violate this convention. Registering `Flink_NullIfZero` requires a Rust change, expanding this Java-only PR's scope. Deferred to #1857 where the runtime pipeline is wired and the Rust+Java change belongs naturally.

---

## 9. Scope Boundaries

### In Scope
- `RexInputRefConverter`, `RexLiteralConverter`, `RexCallConverter`
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Unary: `-`, `+` (identity)
- Basic numeric CAST (INT, BIGINT, FLOAT, DOUBLE, DECIMAL)
- Unit tests (24 tests)

### Out of Scope
- Logical operators (#1860)
- Comparison operators (#1861)
- Full CAST support — string, date, timestamp (#1864)
- Math functions — ABS, SQRT, etc. (new issue)
- Converter registration wiring (#1857)
- Decimal precision/scale promotion (follow-up if needed)
- Integration tests (require #1857 FlinkAuronCalcOperator first)
