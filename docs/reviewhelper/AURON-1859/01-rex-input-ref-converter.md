# Commit 1: RexInputRefConverter — column reference conversion

## What it does

Implements `RexInputRefConverter`, the first concrete `FlinkRexNodeConverter` that converts Calcite `RexInputRef` (column reference by index) into Auron's native `PhysicalExprNode` containing a `PhysicalColumn` with name and index. Also fixes the surefire configuration to discover `*Test.java` files alongside existing `*ITCase.java`.

## Files to review

| File | Key details |
|------|-------------|
| `RexInputRefConverter.java` | 3 methods: `getNodeClass()` → `RexInputRef.class`, `isSupported()` → always true, `convert()` → resolves name from `ConverterContext.getInputType()`, builds `PhysicalColumn{name, index}` |
| `RexInputRefConverterTest.java` | 4 tests: getNodeClass, isSupported, convert index 0, convert index 1. Uses real Calcite `RexBuilder` (not mocks). |
| `pom.xml` | +1 line: adds `**/*Test.java` to surefire includes |

## What to look for

1. The cast `(RexInputRef) node` in `convert()` relies on factory dispatch guarantee (factory only calls convert with matching type). No explicit `instanceof` guard — matches the framework pattern.
2. The `@throws IllegalArgumentException` Javadoc is inherited from the base `FlinkNodeConverter.convert()` interface — the actual runtime would be `ClassCastException` if bypassing the factory. Accepted as framework-level guarantee.
3. The surefire fix also enables `FlinkNodeConverterFactoryTest` from PR #2146 which was previously silently excluded from CI — verified it passes.