# Design: Flink Node Converter Tools (AURON #1856)

**Issue**: https://github.com/apache/auron/issues/1856
**AIP**: AIP-1 — Introduce Flink integration of native engine
**Author**: @weiqingy
**Status**: Draft (Rev 3 — expression-level architecture per reviewer feedback)

## Revision History

| Rev | Date | Summary |
|-----|------|---------|
| 1 | 2026-03-18 | Initial design: ExecNode-level converter with constructor-injected factory |
| 2 | 2026-03-21 | Singleton factory, input schema in context, `PhysicalPlanNode` return type (per reviewer Q1-Q3) |
| 3 | 2026-03-30 | **Architecture shift**: expression-level converters (RexNode/AggregateCall), sub-interfaces, `PhysicalExprNode` return type (per reviewer March 23 + March 24 feedback) |

## 1. Motivation

Per AIP-1, the Flink integration requires a **planner-side framework** that converts Flink physical plan nodes (`ExecNode` tree) into Auron's native execution plan for Rust/DataFusion execution. The data exchange layer is now in place:

```
Flink RowData -> Arrow (Writer #1850) -> Native Engine -> Arrow -> Flink RowData (Reader #1851 DONE)
```

What's missing is the **conversion infrastructure** — the machinery that decides *which* Flink expressions and aggregates can be converted and *how* to translate them into native plan representations. This is analogous to Spark's `NativeConverters.convertExpr()` pattern and Gluten's `RexNodeConverter` + `RexCallConverterFactory` pattern.

### Why Expression-Level (Rev 3 Architecture Shift)

The Rev 2 design centered on `ExecNode`-level conversion (`FlinkNodeConverter` converts `ExecNode` -> `PhysicalPlanNode`). Based on reviewer feedback (March 23), this is **too abstract** for two reasons:

1. **ExecNode encompasses too many concepts** — converting at that level requires internal cross-references within the converter, mixing expression conversion with plan assembly.
2. **RexNode-level converters maximize reusability** — `RexLiteral` and `RexInputRef` converters can be shared across Calc, Agg, and future operators. It's hard to unify Calc and Agg at the ExecNode level.

The top-level `PhysicalPlanNode` assembly (e.g., building a `ProjectionExecNode` from converted expressions) happens **outside** the expression converters — in the future rewritten `StreamExecCalc` or `ExecNodeGraphProcessor`.

This issue now delivers **five foundational classes**:

| Class | Role |
|-------|------|
| `FlinkNodeConverter<T>` | Base interface defining the generic convert contract |
| `FlinkRexNodeConverter` | Sub-interface: converts Flink `RexNode` -> native `PhysicalExprNode` |
| `FlinkAggCallConverter` | Sub-interface: converts Flink `AggregateCall` -> native `PhysicalExprNode` |
| `FlinkNodeConverterFactory` | Singleton registry that dispatches by input type (RexNode vs AggregateCall) |
| `ConverterContext` | Holds shared state (configuration, input schema, classloader) needed during conversion |

These classes are **framework only** — no actual converter implementations (e.g., `RexCallConverter`, `RexLiteralConverter`) are included. Subsequent issues will implement converters that plug into this framework.

## 2. Design Approach

### Two Candidate Approaches

**Approach A — Expression-level converters with typed sub-interfaces (recommended)**

A generic base interface `FlinkNodeConverter<T>` defines the conversion contract. Two sub-interfaces specialize it for `RexNode` and `AggregateCall`. A singleton factory holds separate registries for each input type and dispatches by class hierarchy.

This draws on best practices from:
- **Gluten** (`RexCallConverterFactory` + `RexCallConverter`): singleton factory, `isSuitable()` + `toTypedExpr()` pattern, expression-level granularity
- **Spark Auron** (`NativeConverters.convertExpr()` + `convertAggregateExpr()`): expression -> `PhysicalExprNode` conversion, aggregate -> `PhysicalExprNode` (wrapping `PhysicalAggExprNode`)

**Approach B — ExecNode-level converter (Rev 2 design)**

A single converter interface converts `ExecNode` -> `PhysicalPlanNode`, with expression conversion handled internally.

**Rejected because** (reviewer March 23 feedback):
- ExecNode is too abstract — mixes plan assembly with expression conversion
- Hard to unify Calc and Agg at the ExecNode level
- Cannot reuse expression converters across operators

### Decision: Approach A

Expression-level converters are more modular, more testable, and maximize code reuse across operator types. The plan-level assembly is a separate concern for the future `ExecNodeGraphProcessor`.

## 3. Detailed Design

### 3.1 Package Structure

All classes in `auron-flink-planner`, matching the planner module's role:

```
auron-flink-extension/auron-flink-planner/src/main/java/
  org/apache/auron/flink/table/planner/converter/
  ├── FlinkNodeConverter.java            (base interface, generic)
  ├── FlinkRexNodeConverter.java         (sub-interface for RexNode)
  ├── FlinkAggCallConverter.java         (sub-interface for AggregateCall)
  ├── FlinkNodeConverterFactory.java     (singleton registry + dispatch)
  └── ConverterContext.java              (shared conversion state)
```

### 3.2 FlinkNodeConverter (Base Interface)

```java
package org.apache.auron.flink.table.planner.converter;

import org.apache.auron.protobuf.PhysicalExprNode;

/**
 * Base interface for converting Flink plan elements to Auron native
 * {@link PhysicalExprNode} representations.
 *
 * <p>This interface is parameterized by the input type to support different
 * categories of plan elements. Two sub-interfaces are provided:
 * <ul>
 *   <li>{@link FlinkRexNodeConverter} for Calcite {@code RexNode} expressions
 *   <li>{@link FlinkAggCallConverter} for Calcite {@code AggregateCall} aggregates
 * </ul>
 *
 * @param <T> the type of plan element this converter handles
 */
public interface FlinkNodeConverter<T> {

    /**
     * Returns the concrete class this converter handles.
     * Used by {@link FlinkNodeConverterFactory} for lookup dispatch.
     */
    Class<? extends T> getNodeClass();

    /**
     * Checks whether the given element can be converted to native execution.
     *
     * <p>A converter may decline based on unsupported types, operand
     * combinations, or configuration. This method must not have side effects.
     */
    boolean isSupported(T node, ConverterContext context);

    /**
     * Converts the given element to a native {@link PhysicalExprNode}.
     *
     * @throws IllegalArgumentException if the element type does not match
     *         {@link #getNodeClass()}
     */
    PhysicalExprNode convert(T node, ConverterContext context);
}
```

**Design rationale**:

- **Generic type parameter `<T>`** — Enables type-safe sub-interfaces without casts. `FlinkRexNodeConverter` works with `RexNode`; `FlinkAggCallConverter` works with `AggregateCall`. This follows the reviewer's suggestion of a base class with typed sub-interfaces.

- **Returns `PhysicalExprNode`** — Both RexNode expressions and AggregateCall aggregates map to `PhysicalExprNode` in the protobuf. In `auron.proto`, `PhysicalExprNode` is a oneof containing both scalar expression types (literal, column, binary op, cast, etc.) and `PhysicalAggExprNode`. This matches Spark's `NativeConverters.convertExpr()` (returns `PhysicalExprNode`) and `convertAggregateExpr()` (also returns `PhysicalExprNode` wrapping `PhysicalAggExprNode`).

- **`getNodeClass()`** — Enables the factory to build type-based lookup maps. Analogous to Gluten's operator-name-keyed map in `RexCallConverterFactory`.

- **`isSupported(node, context)`** — Separated from `convert()` so the caller can check feasibility without side effects. Takes `ConverterContext` because support decisions depend on the **input schema** (are the operand types supported?) and **configuration flags**. Mirrors Gluten's `RexCallConverter.isSuitable(RexCall, context)`.

### 3.3 FlinkRexNodeConverter (Sub-Interface)

```java
package org.apache.auron.flink.table.planner.converter;

import org.apache.calcite.rex.RexNode;

/**
 * Converts a Calcite {@link RexNode} expression to an Auron native
 * {@link org.apache.auron.protobuf.PhysicalExprNode}.
 *
 * <p>Implementations handle specific RexNode subtypes:
 * <ul>
 *   <li>{@code RexLiteral} -> scalar literal values
 *   <li>{@code RexInputRef} -> column references (resolved via
 *       {@link ConverterContext#getInputType()})
 *   <li>{@code RexCall} -> function/operator calls (arithmetic, comparison,
 *       CAST, etc.)
 *   <li>{@code RexFieldAccess} -> nested field access
 * </ul>
 *
 * <p>RexNode converters are reusable across operator types — the same
 * {@code RexInputRef} converter works for Calc projections, Agg grouping
 * expressions, and future operators.
 */
public interface FlinkRexNodeConverter extends FlinkNodeConverter<RexNode> {
}
```

**Design rationale**:

- **Extends `FlinkNodeConverter<RexNode>`** — Inherits the generic contract with `RexNode` as the input type. No additional methods needed at this point; the sub-interface serves as a type marker for the factory's `rexConverterMap`.

- **Reusability is the key motivation** — As the reviewer noted, `RexLiteral` and `RexInputRef` converters are needed by Calc (projections, conditions), Agg (grouping expressions), and future operators. Operating at the RexNode level means one converter implementation serves all operators. This matches Gluten's `RexNodeConverter` design.

- **Future concrete implementations** (not in this PR) would include:
  - `FlinkRexLiteralConverter` — `RexLiteral` -> `ScalarValue`
  - `FlinkRexInputRefConverter` — `RexInputRef` -> `PhysicalColumn` (using `ConverterContext.getInputType()`)
  - `FlinkRexCallConverter` — `RexCall` -> function-specific conversion (arithmetic, CAST, comparison, etc.)

### 3.4 FlinkAggCallConverter (Sub-Interface)

```java
package org.apache.auron.flink.table.planner.converter;

import org.apache.calcite.rel.core.AggregateCall;

/**
 * Converts a Calcite {@link AggregateCall} to an Auron native
 * {@link org.apache.auron.protobuf.PhysicalExprNode} (wrapping a
 * {@code PhysicalAggExprNode}).
 *
 * <p>An {@code AggregateCall} represents an aggregate function invocation
 * (e.g., SUM, COUNT, MAX) with its argument references, return type, and
 * distinctness. The converter translates this into a
 * {@code PhysicalAggExprNode} containing the aggregate function type,
 * converted child expressions, and return type.
 *
 * <p>Note: AggregateCall internally references input columns by index.
 * The converter uses {@link ConverterContext#getInputType()} to resolve
 * these indices to concrete types for type checking and cast insertion.
 */
public interface FlinkAggCallConverter extends FlinkNodeConverter<AggregateCall> {
}
```

**Design rationale**:

- **Extends `FlinkNodeConverter<AggregateCall>`** — Separate from `FlinkRexNodeConverter` because `AggregateCall` is not a `RexNode`. Flink/Calcite's `AggregateCall` contains `SqlAggFunction`, argument indices, return type, and distinctness — structurally different from `RexNode` subtypes.

- **Returns `PhysicalExprNode`** — In `auron.proto`, `PhysicalAggExprNode` is a variant within `PhysicalExprNode.agg_expr`. Spark's `NativeConverters.convertAggregateExpr()` follows the same pattern: builds `PhysicalAggExprNode` -> wraps in `PhysicalExprNode`. This keeps the return type uniform across all converter sub-interfaces.

- **Reuses RexNode converters** — An `AggregateCall` references `RexLiteral` and `RexInputRef` for its arguments. Future concrete implementations of this interface will delegate to the registered `FlinkRexNodeConverter` implementations for argument conversion, maximizing reuse.

### 3.5 ConverterContext

```java
package org.apache.auron.flink.table.planner.converter;

import org.apache.auron.configuration.AuronConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.types.logical.RowType;

/**
 * Provides shared state to {@link FlinkNodeConverter} implementations
 * during conversion.
 *
 * <p>Carries the input schema, configuration, and classloader needed for
 * type-aware conversion of Flink expressions and aggregate calls.
 */
public class ConverterContext {

    private final ReadableConfig tableConfig;
    private final AuronConfiguration auronConfiguration;
    private final ClassLoader classLoader;
    private final RowType inputType;

    public ConverterContext(
            ReadableConfig tableConfig,
            AuronConfiguration auronConfiguration,
            ClassLoader classLoader,
            RowType inputType) { ... }

    /** Flink table-level configuration. */
    public ReadableConfig getTableConfig() { ... }

    /** Auron-specific configuration (batch size, memory fraction, enable flags, etc.). */
    public AuronConfiguration getAuronConfiguration() { ... }

    /** ClassLoader for the current Flink context. */
    public ClassLoader getClassLoader() { ... }

    /** Input schema of the node being converted. */
    public RowType getInputType() { ... }
}
```

**Unchanged from Rev 2.** See Rev 2 design for full rationale.

| Field | Source | Why converters need it |
|-------|--------|----------------------|
| `RowType inputType` | From the ExecNode's input edge output type | Resolve `RexInputRef` column references to names/types; check type support; determine if casts are needed |
| `ReadableConfig tableConfig` | `PlannerBase.getTableConfig()` | Needed by `ExecNodeContext.newPersistedConfig()` when creating nodes |
| `AuronConfiguration auronConfiguration` | `AuronAdaptor.getInstance().getAuronConfiguration()` | Auron-specific settings (enable flags, batch size, etc.) |
| `ClassLoader classLoader` | `PlannerBase.getFlinkContext().getClassLoader()` | Needed by Flink's code generation and service loading |

### 3.6 FlinkNodeConverterFactory (Singleton)

```java
package org.apache.auron.flink.table.planner.converter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.core.AggregateCall;

/**
 * Singleton registry of {@link FlinkNodeConverter} instances. Dispatches
 * conversion requests to the appropriate converter based on the input
 * element's class.
 *
 * <p>Maintains separate registries for different converter categories:
 * <ul>
 *   <li>{@code rexConverterMap} for {@link FlinkRexNodeConverter} instances
 *       (keyed by {@code RexNode} subclass)
 *   <li>{@code aggConverterMap} for {@link FlinkAggCallConverter} instances
 *       (keyed by {@code AggregateCall} class)
 * </ul>
 *
 * <p>This design follows the reviewer's proposal and Gluten's
 * {@code RexCallConverterFactory} pattern.
 *
 * <p>Usage:
 * <pre>
 *   FlinkNodeConverterFactory factory = FlinkNodeConverterFactory.getInstance();
 *   // Convert a RexNode expression
 *   Optional&lt;PhysicalExprNode&gt; result = factory.convertRexNode(rexNode, context);
 *   // Convert an AggregateCall
 *   Optional&lt;PhysicalExprNode&gt; aggResult = factory.convertAggCall(aggCall, context);
 * </pre>
 */
public class FlinkNodeConverterFactory {

    private static final FlinkNodeConverterFactory INSTANCE = new FlinkNodeConverterFactory();

    private final Map<Class<? extends RexNode>, FlinkRexNodeConverter> rexConverterMap;
    private final Map<Class<? extends AggregateCall>, FlinkAggCallConverter> aggConverterMap;

    private FlinkNodeConverterFactory() {
        this.rexConverterMap = new HashMap<>();
        this.aggConverterMap = new HashMap<>();
    }

    /** Returns the singleton instance. */
    public static FlinkNodeConverterFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Registers a RexNode converter for its declared RexNode subclass.
     *
     * @throws IllegalArgumentException if a converter is already registered
     *         for the same RexNode class
     */
    public void registerRexConverter(FlinkRexNodeConverter converter) {
        Class<? extends RexNode> nodeClass = converter.getNodeClass();
        if (rexConverterMap.containsKey(nodeClass)) {
            throw new IllegalArgumentException(
                    "Duplicate RexNode converter for " + nodeClass.getName());
        }
        rexConverterMap.put(nodeClass, converter);
    }

    /**
     * Registers an AggregateCall converter.
     *
     * @throws IllegalArgumentException if a converter is already registered
     *         for the same AggregateCall class
     */
    public void registerAggConverter(FlinkAggCallConverter converter) {
        Class<? extends AggregateCall> nodeClass = converter.getNodeClass();
        if (aggConverterMap.containsKey(nodeClass)) {
            throw new IllegalArgumentException(
                    "Duplicate AggregateCall converter for " + nodeClass.getName());
        }
        aggConverterMap.put(nodeClass, converter);
    }

    /**
     * Attempts to convert the given RexNode to a native PhysicalExprNode.
     *
     * <p>Returns the native expression if a matching converter exists and
     * supports the node. Returns empty if no converter, not supported, or
     * conversion fails (fail-safe).
     */
    public Optional<PhysicalExprNode> convertRexNode(RexNode node, ConverterContext context) {
        FlinkRexNodeConverter converter = rexConverterMap.get(node.getClass());
        if (converter == null) {
            return Optional.empty();
        }
        if (!converter.isSupported(node, context)) {
            return Optional.empty();
        }
        try {
            return Optional.of(converter.convert(node, context));
        } catch (Exception e) {
            // Log warning, return empty (fail-safe)
            return Optional.empty();
        }
    }

    /**
     * Attempts to convert the given AggregateCall to a native PhysicalExprNode.
     */
    public Optional<PhysicalExprNode> convertAggCall(
            AggregateCall aggCall, ConverterContext context) {
        FlinkAggCallConverter converter = aggConverterMap.get(aggCall.getClass());
        if (converter == null) {
            return Optional.empty();
        }
        if (!converter.isSupported(aggCall, context)) {
            return Optional.empty();
        }
        try {
            return Optional.of(converter.convert(aggCall, context));
        } catch (Exception e) {
            // Log warning, return empty (fail-safe)
            return Optional.empty();
        }
    }

    /**
     * Returns the converter registered for the given element's class, if any.
     * Dispatches by type hierarchy: checks RexNode first, then AggregateCall.
     */
    public Optional<FlinkNodeConverter<?>> getConverter(Class<?> nodeClass) {
        if (RexNode.class.isAssignableFrom(nodeClass)) {
            return Optional.ofNullable(rexConverterMap.get(nodeClass));
        } else if (AggregateCall.class.isAssignableFrom(nodeClass)) {
            return Optional.ofNullable(aggConverterMap.get(nodeClass));
        }
        return Optional.empty();
    }
}
```

**Design rationale**:

- **Separate maps** — `rexConverterMap` and `aggConverterMap` provide type-safe storage. This follows the reviewer's proposed code structure (March 23 comment) where the factory dispatches by `RexNode.class.isAssignableFrom()` vs `AggregateCall.class.isAssignableFrom()`.

- **Typed registration and conversion methods** — `registerRexConverter()` / `convertRexNode()` and `registerAggConverter()` / `convertAggCall()` provide type-safe APIs without casts. Callers know exactly what they're registering and converting.

- **`getConverter(Class<?>)`** — General-purpose lookup that dispatches by type hierarchy, matching the reviewer's code snippet. Returns `Optional<FlinkNodeConverter<?>>` (wildcard generic) for flexibility.

- **Singleton, fail-safe, duplicate detection** — Unchanged from Rev 2. See Rev 2 design for full rationale.

- **Extensible** — As the reviewer noted, "we could also explore ways to expand this further." Future categories (e.g., `RexFieldAccess`, sort expressions) can be added with new maps and registration methods without modifying existing converters.

## 4. Interaction Diagram

How these five classes fit into the future end-to-end flow:

```
                      Future ExecNodeGraphProcessor (NOT in this PR)
                     ┌─────────────────────────────────────────────────┐
                     │ For each ExecNode in graph:                     │
                     │  1. Build ConverterContext (input RowType)       │
                     │  2. Extract expressions from node               │
                     │     - Calc: List<RexNode> proj, RexNode cond    │
                     │     - Agg: List<AggregateCall>, grouping exprs  │
                     │  3. Convert each via factory:                    │
                     │     factory.convertRexNode(rex, ctx)             │
                     │     factory.convertAggCall(agg, ctx)             │
                     │  4. Assemble PhysicalPlanNode from results       │
                     │  5. Wrap in AuronExecNode, reconnect edges       │
                     └──────────────┬──────────────────────────────────┘
                                    │ uses
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
          ┌──────────────┐  ┌─────────────────┐  ┌─────────────────────────┐
          │ConverterCtx  │  │ConverterFactory │  │ FlinkNodeConverter<T>   │
          │              │  │  (singleton)     │  │   (base interface)      │
          │ inputType    │  │                  │  │                         │
          │ tableConfig  │  │ convertRexNode() │  │ getNodeClass()          │
          │ auronConfig  │  │ convertAggCall() │  │ isSupported(T, ctx)     │
          │ classLoader  │  │ getConverter()   │  │ convert(T, ctx)         │
          └──────────────┘  └─────────────────┘  │   -> PhysicalExprNode   │
                                                  └────────────┬────────────┘
                                                               │ extends
                                              ┌────────────────┼────────────────┐
                                              ▼                                 ▼
                                ┌──────────────────────┐          ┌──────────────────────┐
                                │ FlinkRexNodeConverter │          │ FlinkAggCallConverter  │
                                │   <RexNode>           │          │   <AggregateCall>      │
                                └──────────┬───────────┘          └──────────┬─────────────┘
                                           │ implements (future)             │ implements (future)
                              ┌────────────┼────────────┐                   │
                              ▼            ▼            ▼                   ▼
                        ┌──────────┐ ┌──────────┐ ┌──────────┐      ┌──────────────┐
                        │RexLiteral│ │RexInput  │ │RexCall   │      │AggCall       │
                        │Converter │ │RefConv.  │ │Converter │      │Converter     │
                        │(future)  │ │(future)  │ │(future)  │      │(future)      │
                        │          │ │          │ │          │      │              │
                        │Shared by:│ │Shared by:│ │Shared by:│      │SUM,COUNT,MAX │
                        │Calc, Agg,│ │Calc, Agg,│ │Calc, Agg,│      │AVG, etc.     │
                        │Filter,...│ │Filter,...│ │Filter,...│      │              │
                        └──────────┘ └──────────┘ └──────────┘      └──────────────┘
```

**Key insight**: RexNode converters (left branch) are shared across operator types. A `RexInputRefConverter` registered once works for Calc projections, Agg grouping expressions, filter conditions, and any future operator. This is the reusability benefit of the expression-level architecture.

## 5. Prior Art Comparison

| Aspect | Spark Auron | Gluten Flink | This PR (Rev 3) |
|--------|------------|-------------|-----------------|
| Converter interface | N/A (direct `convertExpr` function) | `RexCallConverter` | `FlinkNodeConverter<T>` (base) + `FlinkRexNodeConverter` + `FlinkAggCallConverter` |
| Factory | N/A (single `NativeConverters` object) | `RexCallConverterFactory` (singleton) | `FlinkNodeConverterFactory` (singleton, multi-map) |
| Granularity | Expression-level (`convertExpr`, `convertAggregateExpr`) | Expression-level (`RexNode`, `RexCall`) | Expression-level (`RexNode`, `AggregateCall`) |
| Discovery | N/A (hardcoded pattern match) | Static registration | `registerRexConverter()` / `registerAggConverter()` |
| Context | Implicit (accessed via SparkPlan) | `RexConversionContext` (input attr names) | `ConverterContext` (input RowType + config) |
| Input schema | Not in context | `inputAttributeNames: List<String>` | `inputType: RowType` (names + types) |
| Return type | `PhysicalExprNode` | `TypedExpr` (Velox native) | `PhysicalExprNode` |
| Agg handling | `convertAggregateExpr()` -> `PhysicalExprNode` | `AggregateCallConverter` (separate) | `FlinkAggCallConverter` -> `PhysicalExprNode` |
| Fail-safe | Throws `NotImplementedError` | Throws on unsupported | `Optional.empty()` on failure |

**Key alignment with prior art**:
- **Expression-level granularity** — matches both Spark (`convertExpr`) and Gluten (`RexNodeConverter`)
- **`PhysicalExprNode` return type** — matches Spark's `NativeConverters.convertExpr()` and `convertAggregateExpr()` (both return `PhysicalExprNode`). Confirmed by reviewer (March 24).
- **Agg as `PhysicalExprNode`** — In `auron.proto`, `PhysicalAggExprNode` is a variant within `PhysicalExprNode.agg_expr`. Spark builds `PhysicalAggExprNode` -> wraps in `PhysicalExprNode`. Same pattern here.
- **Singleton factory with separate maps** — matches reviewer's proposed code structure and Gluten's `RexCallConverterFactory`

## 6. Dependencies

### Build Dependencies (already in auron-flink-planner pom.xml)

- `auron-flink-runtime` — provides `AuronConfiguration`, `PhysicalExprNode` (protobuf)
- `flink-table-planner` — provides `ExecNode`, `ExecNodeBase`, and transitively Calcite's `RexNode`, `AggregateCall`
- `flink-table-runtime` — provides `RowData`, `ReadableConfig`
- `flink-table-calcite-bridge` — Calcite bridge (already declared)
- JUnit Jupiter — test scope

### New Dependencies: None

### Code Dependencies

- `org.apache.calcite.rex.RexNode` — Calcite expression base class
- `org.apache.calcite.rel.core.AggregateCall` — Calcite aggregate call
- `org.apache.flink.table.types.logical.RowType` — Flink's row type (input schema)
- `org.apache.flink.configuration.ReadableConfig` — Flink configuration interface
- `org.apache.auron.configuration.AuronConfiguration` — Auron configuration base class
- `org.apache.auron.protobuf.PhysicalExprNode` — Auron's native expression protobuf

## 7. Test Plan

### 7.1 FlinkNodeConverterFactoryTest

| Test | What It Validates |
|------|-------------------|
| `testEmptyFactoryRexNode` | Factory with no converters returns empty for any RexNode |
| `testEmptyFactoryAggCall` | Factory with no converters returns empty for any AggregateCall |
| `testRexConverterDispatch` | Registered mock RexNode converter is called for matching RexNode type |
| `testAggConverterDispatch` | Registered mock AggCall converter is called for matching AggregateCall type |
| `testUnsupportedRexPassthrough` | RexNode converter's `isSupported` returns false -> empty returned |
| `testConversionFailureFallback` | Converter throws -> empty returned (fail-safe) |
| `testDuplicateRexConverterRejected` | Two converters for same RexNode class -> `IllegalArgumentException` |
| `testDuplicateAggConverterRejected` | Two converters for same AggregateCall class -> `IllegalArgumentException` |
| `testGetConverterByClass` | `getConverter(RexLiteral.class)` returns registered RexNode converter |
| `testGetConverterAbsent` | `getConverter` returns empty for unregistered class |

### 7.2 ConverterContextTest

| Test | What It Validates |
|------|-------------------|
| `testConstruction` | All four fields accessible via getters |
| `testNullTableConfigRejected` | Constructor rejects null `tableConfig` |
| `testNullClassLoaderRejected` | Constructor rejects null `classLoader` |
| `testNullInputTypeRejected` | Constructor rejects null `inputType` |

### Build & Run

```bash
./build/mvn test -pl auron-flink-extension/auron-flink-planner -am \
  -Pscala-2.12 -Pflink-1.18 -Pspark-3.5 -Prelease \
  -Dtest=FlinkNodeConverterFactoryTest,ConverterContextTest
```

## 8. Alternatives Considered

### 8.1 ExecNode-level converter (Rev 2 design)

`FlinkNodeConverter.convert(ExecNode<?>) -> PhysicalPlanNode`

**Rejected** (reviewer March 23):
- ExecNode is too abstract — requires internal cross-references within converters
- Hard to unify Calc and Agg at the ExecNode level
- Cannot reuse expression converters across operators
- See Section 2 for details

### 8.2 Non-generic sub-interfaces (no base interface)

Two independent interfaces (`FlinkRexNodeConverter`, `FlinkAggCallConverter`) without a shared base.

**Rejected because**:
- Loses the ability to express the common contract (`getNodeClass`, `isSupported`, `convert`)
- The reviewer explicitly proposed "FlinkNodeConverter is the base class" with sub-interfaces
- The factory's `getConverter(Class<?>)` needs a common return type

### 8.3 Single unified map in factory

One `Map<Class<?>, FlinkNodeConverter<?>>` instead of separate `rexConverterMap` and `aggConverterMap`.

**Rejected because**:
- Loses type safety — `FlinkRexNodeConverter` and `FlinkAggCallConverter` would be mixed
- The reviewer's code snippet explicitly shows separate maps: `rexConverterMap.get(node)` vs `aggConverterMap.get(node.getClass())`
- Separate maps enable typed registration (`registerRexConverter` vs `registerAggConverter`)

### 8.4 Visitor-based tree rewriter

**Rejected** — unchanged from Rev 2. See Rev 2 Section 8.2.

### 8.5 Holding PlannerBase in ConverterContext

**Rejected** — unchanged from Rev 2. See Rev 2 Section 8.3.

## 9. Scope Boundaries

### In Scope
- `FlinkNodeConverter<T>` base interface
- `FlinkRexNodeConverter` sub-interface
- `FlinkAggCallConverter` sub-interface
- `FlinkNodeConverterFactory` singleton class (with separate maps)
- `ConverterContext` class (with input schema)
- Unit tests for factory and context

### Out of Scope (follow-up issues)
- `ExecNodeGraphProcessor` implementation (graph traversal + wiring)
- Actual converter implementations (`RexLiteralConverter`, `RexInputRefConverter`, `RexCallConverter`, etc.)
- Plan-level `PhysicalPlanNode` assembly (building `ProjectionExecNode` from converted expressions)
- Flink configuration flags (e.g., `ENABLE_CALC`)
- `FlinkMetricNode` class
- Changes to existing runtime classes
