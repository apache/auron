# Review Helper — Commit 01: Flink Expression-Level Converter Framework

## What This Commit Does

Introduces five foundational classes in `auron-flink-planner` that form the expression-level converter framework for Flink's native query acceleration. A generic base interface `FlinkNodeConverter<T>` defines the conversion contract. Two sub-interfaces specialize it for `RexNode` expressions and `AggregateCall` aggregates. A singleton factory holds separate registries for each type and dispatches conversions. A context object carries the input schema and configuration needed during conversion.

This is framework-only — no actual converter implementations are included.

## Files to Review

| File | Key Points |
|------|-----------|
| `FlinkNodeConverter.java` | Generic `<T>` base interface. Returns `PhysicalExprNode`. Three methods: `getNodeClass()`, `isSupported()`, `convert()`. |
| `FlinkRexNodeConverter.java` | Sub-interface for `RexNode`. Type marker only — no additional methods. Javadoc lists future implementations. |
| `FlinkAggCallConverter.java` | Sub-interface for `AggregateCall`. Type marker only. Separate from RexNode because AggregateCall is not a RexNode. |
| `FlinkNodeConverterFactory.java` | Singleton with dual maps (`rexConverterMap`, `aggConverterMap`). Typed registration + dispatch. Fail-safe: catches exceptions, returns `Optional.empty()`. Package-private constructor for test isolation. |
| `ConverterContext.java` | Immutable. 4 fields: `ReadableConfig`, `AuronConfiguration` (nullable), `ClassLoader`, `RowType`. Null-checks on required fields. |
| `FlinkNodeConverterFactoryTest.java` | 8 tests. Uses stub classes for `RexLiteral` and `AggregateCall`. Tests: rex/agg dispatch, unsupported passthrough, fail-safe on exception, duplicate rejection, converter lookup for both rex and agg paths. |

## What to Look For

- **Generic type safety**: The `FlinkNodeConverter<T>` → sub-interface pattern avoids unchecked casts. One `@SuppressWarnings("unchecked")` in `getConverter()` — justified because the cast is guarded by `isAssignableFrom()`.
- **Singleton vs test isolation**: Factory constructor is package-private so tests create fresh instances. The static `INSTANCE` field is for production use.
- **Fail-safe conversion**: `convertRexNode()` and `convertAggCall()` catch all exceptions and return empty. This matches the design doc's requirement that unsupported nodes gracefully fall back.
- **AggregateCall map key**: Currently keyed by `Class<? extends AggregateCall>`. In practice, most registrations will use `AggregateCall.class` directly. Future evolution may key by `SqlAggFunction` instead.
