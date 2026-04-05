# Commit 3: RexCallConverter — arithmetic, unary minus, cast with type promotion

## What it does

Implements `RexCallConverter`, the most complex converter handling binary arithmetic (+,-,*,/,%), unary minus/plus, and CAST. Features explicit type promotion via `getCommonTypeForComparison()` + `castIfNecessary()` using `PhysicalTryCastNode`, following the reviewer's PoC pattern exactly.

## Files to review

| File | Key details |
|------|-------------|
| `RexCallConverter.java` | Constructor takes `FlinkNodeConverterFactory` for recursive operand conversion. `isSupported()` checks SqlKind + numeric guard. `buildBinaryExpr()` computes compatible type, casts operands, wraps result if output type differs. Uses `FlinkTypeFactory.toLogicalType()` + `SchemaConverters.convertToAuronArrowType()` for Arrow type mapping. |
| `RexCallConverterTest.java` | 17 tests: 5 binary ops, unary minus/plus, cast, mixed type promotion (INT+BIGINT), output type cast, nested expressions, isSupported guards, and 3 direct `getCommonTypeForComparison` tests (DECIMAL wins, DOUBLE fallback, incompatible returns null). |

## What to look for

1. **Type promotion logic** matches reviewer PoC blocks 1-3 exactly: `getCommonTypeForComparison()` (same-type shortcut, DECIMAL wins, BIGINT if both exact, else DOUBLE), `castIfNecessary()` (compare by SqlTypeName, wrap in TryCast), output type cast.
2. **V1 bug fix**: `getCommonTypeForComparison` returns `null` for incompatible types → `buildBinaryExpr` throws `IllegalStateException` (not silent fallback).
3. **`FlinkTypeFactory.toLogicalType()`** is Flink internal API (Scala class, static method callable from Java). Used per reviewer's PoC pattern.
4. **Static `TYPE_FACTORY`** — `SqlTypeFactoryImpl` is a static constant (V1 fix, not per-call).
5. **`isSupported()` numeric guard** — binary arithmetic kinds additionally check `SqlTypeUtil.isNumeric(call.getType())` to reject TIMESTAMP + INTERVAL.
