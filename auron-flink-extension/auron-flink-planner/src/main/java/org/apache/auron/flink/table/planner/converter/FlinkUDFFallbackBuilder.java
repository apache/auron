/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron.flink.table.planner.converter;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.auron.flink.functions.FlinkUDFPayload;
import org.apache.auron.flink.utils.SchemaConverters;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalUDFWrapperExprNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.ExtractionUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Packages a call to a user-defined Flink {@link ScalarFunction} into the native UDF wrapper
 * expression node, so that the surrounding Calc keeps running natively while the function itself is
 * evaluated by an upcall into the JVM.
 *
 * <p>The wrapper carries the generated invoker for the call, the function instance it holds, and
 * the argument and return types inside an opaque serialized blob, plus one native parameter
 * expression per {@code eval} argument. It is emitted in place of the call, so the native
 * parent's own evaluation semantics govern it.
 *
 * <p>This is a coverage feature rather than a performance one: a JVM upcall per batch is slower
 * than a native expression, whatever the invocation inside it costs. What it buys is that the rest
 * of the Calc no longer falls back along with the function.
 *
 * <p>A function overriding {@code open} or {@code close} is admitted. The runtime retains one
 * wrapper per subtask, so each hook runs once for that subtask, which is the lifecycle Flink gives
 * a function on its own path.
 */
public final class FlinkUDFFallbackBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUDFFallbackBuilder.class);

    /**
     * The argument and return types a wrapper can carry. It is the intersection of the Flink-to-Arrow
     * tables the wrapper crosses: the plan-side Arrow type mapping, the Java Arrow schema the wrapper
     * builds its vectors from, and the per-field Arrow reader and writer. A type outside the set is
     * declined here, at plan time, because the alternative is worse: a type the native plan schema
     * cannot express surfaces at execution as an empty result set rather than as an error.
     *
     * <p>{@code TIMESTAMP} and {@code TIMESTAMP_LTZ} are excluded even though both tables accept
     * them, because the two disagree on the Arrow time unit above millisecond precision, and Flink's
     * bare {@code TIMESTAMP} is {@code TIMESTAMP(6)}.
     */
    private static final Set<LogicalTypeRoot> SUPPORTED_TYPE_ROOTS = EnumSet.of(
            LogicalTypeRoot.BOOLEAN,
            LogicalTypeRoot.TINYINT,
            LogicalTypeRoot.SMALLINT,
            LogicalTypeRoot.INTEGER,
            LogicalTypeRoot.BIGINT,
            LogicalTypeRoot.FLOAT,
            LogicalTypeRoot.DOUBLE,
            LogicalTypeRoot.CHAR,
            LogicalTypeRoot.VARCHAR,
            LogicalTypeRoot.BINARY,
            LogicalTypeRoot.VARBINARY,
            LogicalTypeRoot.DECIMAL,
            LogicalTypeRoot.DATE);

    private FlinkUDFFallbackBuilder() {}

    /**
     * Returns the user-defined scalar function behind the given call, or empty if the call is not one.
     *
     * <p>The test is on operator identity, never on {@link org.apache.calcite.sql.SqlKind}: Flink's
     * own {@code IF}, {@code TRY_CAST} and {@code UNIX_TIMESTAMP} all carry
     * {@code SqlKind.OTHER_FUNCTION}, so a kind-based test would swallow built-ins. Two operator
     * classes are reachable, chosen by the registration API rather than by the Flink version: the
     * deprecated {@code registerFunction} produces a {@link ScalarSqlFunction}, and every other
     * registration path produces a {@link BridgingSqlFunction}.
     *
     * <p>{@link BridgingSqlFunction} also fronts Flink's built-ins, so the narrowing test is on the
     * definition rather than on the operator class. {@code ScalarFunction.getKind()} is final, so a
     * table or aggregate function cannot pass it.
     *
     * @param call the call to inspect
     * @return the user function, or empty for any other operator
     */
    public static Optional<ScalarFunction> userScalarFunctionOf(RexCall call) {
        SqlOperator operator = call.getOperator();
        if (operator instanceof BridgingSqlFunction) {
            return userScalarFunctionOf(((BridgingSqlFunction) operator).getDefinition());
        }
        if (operator instanceof ScalarSqlFunction) {
            return Optional.of(((ScalarSqlFunction) operator).scalarFunction());
        }
        return Optional.empty();
    }

    /**
     * Narrows a function definition to a user scalar function, or empty for anything else.
     *
     * <p>Split out from {@link #userScalarFunctionOf(RexCall)} so it can be exercised directly:
     * {@code BridgingSqlFunction} has no factory reachable outside a running planner, so a test
     * cannot build a call that carries a built-in definition, and every built-in this class must
     * leave alone arrives that way.
     *
     * @param definition the definition behind the operator
     * @return the user function, or empty for a built-in or any non-scalar function
     */
    static Optional<ScalarFunction> userScalarFunctionOf(FunctionDefinition definition) {
        // BridgingSqlFunction also fronts Flink's built-ins, whose definitions are
        // BuiltInFunctionDefinition. Returning one of those as a user function would route an
        // ordinary built-in through a JVM upcall; returning a present-but-null Optional would take
        // out the whole Calc, because the caller runs before any kind check.
        return definition instanceof ScalarFunction ? Optional.of((ScalarFunction) definition) : Optional.empty();
    }

    /**
     * Builds the native wrapper node for a user scalar function call, or returns empty if the call
     * cannot be admitted.
     *
     * <p>Every admission check lives here rather than in the converter's support check, which the
     * converter contract documents as side-effect free and which the factory calls first. Returning
     * empty is the ordinary decline signal: the caller turns it into a fallback to Flink's generated
     * Calc, which produces the correct answer by the slower route.
     *
     * @param call the user function call
     * @param udf the function resolved from the call's operator
     * @param context shared conversion state
     * @param factory the factory used to convert the call's operands into parameter nodes
     * @return the wrapper expression, or empty if any admission check declines
     */
    public static Optional<PhysicalExprNode> build(
            RexCall call, ScalarFunction udf, ConverterContext context, FlinkNodeConverterFactory factory) {
        List<RexNode> operands = call.getOperands();
        LogicalType returnLogicalType = FlinkTypeFactory.toLogicalType(call.getType());
        if (!isSupportedType(returnLogicalType)) {
            return decline(udf, "return type " + returnLogicalType.asSummaryString() + " is not supported");
        }

        DataType[] argTypes = new DataType[operands.size()];
        LogicalType[] argLogicalTypes = new LogicalType[operands.size()];
        for (int i = 0; i < operands.size(); i++) {
            LogicalType argLogicalType =
                    FlinkTypeFactory.toLogicalType(operands.get(i).getType());
            if (!isSupportedType(argLogicalType)) {
                return decline(
                        udf, "argument " + i + " of type " + argLogicalType.asSummaryString() + " is not supported");
            }
            argLogicalTypes[i] = argLogicalType;
            argTypes[i] = TypeConversions.fromLogicalToDataType(argLogicalType);
        }
        DataType returnType = TypeConversions.fromLogicalToDataType(returnLogicalType);

        if (!isEvalAdmissible(udf, argTypes, returnType)) {
            return Optional.empty();
        }
        if (!isPreparable(udf, context)) {
            return Optional.empty();
        }

        List<PhysicalExprNode> params = new ArrayList<>(operands.size());
        for (RexNode operand : operands) {
            Optional<PhysicalExprNode> converted = factory.convertRexNode(operand, context);
            if (!converted.isPresent()) {
                return decline(udf, "argument " + operand + " does not convert to a native expression");
            }
            params.add(converted.get());
        }

        Optional<FlinkUDFCodeGenerator.GeneratedCode> generated = FlinkUDFCodeGenerator.generate(
                call, RowType.of(argLogicalTypes), context.getTableConfig(), context.getClassLoader());
        if (!generated.isPresent()) {
            return Optional.empty();
        }

        byte[] blob;
        try {
            blob = InstantiationUtil.serializeObject(new FlinkUDFPayload(
                    generated.get().getClassName(),
                    generated.get().getCode(),
                    generated.get().getReferences(),
                    argTypes,
                    returnType,
                    udf.getClass().getName(),
                    context.nextUdfWrapperOrdinal()));
        } catch (Exception e) {
            LOG.debug(
                    "Cannot serialize Flink UDF {}; the call falls back.",
                    udf.getClass().getName(),
                    e);
            return Optional.empty();
        }

        return Optional.of(PhysicalExprNode.newBuilder()
                .setUdfWrapperExpr(PhysicalUDFWrapperExprNode.newBuilder()
                        .setSerialized(ByteString.copyFrom(blob))
                        .setReturnType(SchemaConverters.convertToAuronArrowType(returnLogicalType))
                        .setReturnNullable(call.getType().isNullable())
                        .addAllParams(params)
                        .setExprString(call.toString()))
                .build());
    }

    /**
     * Returns whether the given expression sub-tree contains a UDF wrapper anywhere, including at its
     * root.
     *
     * <p>The walk is generic over the protobuf field graph rather than a switch over the expression
     * kinds this converter emits. A hand-written enumeration silently misses any kind left out of it,
     * and a missed kind downgrades a short-circuiting logical node back to one that evaluates its
     * right operand over the whole batch, with no signal that it happened.
     *
     * @param node the expression to scan
     * @return whether a UDF wrapper appears anywhere in the sub-tree
     */
    public static boolean containsUdfWrapper(PhysicalExprNode node) {
        if (node.getExprTypeCase() == PhysicalExprNode.ExprTypeCase.UDF_WRAPPER_EXPR) {
            return true;
        }
        return containsUdfWrapper((Message) node);
    }

    private static boolean containsUdfWrapper(Message message) {
        for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (field.getKey().isRepeated()) {
                for (Object element : (List<?>) field.getValue()) {
                    if (containsNestedWrapper((Message) element)) {
                        return true;
                    }
                }
            } else if (containsNestedWrapper((Message) field.getValue())) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsNestedWrapper(Message message) {
        return message instanceof PhysicalExprNode
                ? containsUdfWrapper((PhysicalExprNode) message)
                : containsUdfWrapper(message);
    }

    private static boolean isSupportedType(LogicalType type) {
        return SUPPORTED_TYPE_ROOTS.contains(type.getTypeRoot());
    }

    /**
     * Returns whether the {@code eval} the call resolves to can be admitted.
     *
     * <p>Which overload runs is Flink's decision, made while the invocation is generated, so no
     * selection happens here. Two shapes are still declined:
     *
     * <ul>
     *   <li>a zero-argument {@code eval}, which produces a zero-column Arrow batch across the
     *       native boundary, a path with no coverage;
     *   <li>any function declaring a varargs {@code eval} the arguments could reach. The generator
     *       handles varargs; what has never been exercised is the packed argument array crossing
     *       that boundary. The test is on the whole overload set rather than on the overload that
     *       would run, so a function declaring both {@code eval(int, int)} and {@code eval(int...)}
     *       declines even though the fixed-arity one would win. That much is deliberate. It does
     *       narrow the admitted set in one shape: a varargs overload whose return type kept it out
     *       of the invokable set was admitted before and declines now.
     * </ul>
     *
     * <p>The last check is Flink's own: it rejects a function whose class or {@code eval} the
     * generated source could not name or call, which is the same requirement the generated call
     * imposes.
     */
    private static boolean isEvalAdmissible(ScalarFunction udf, DataType[] argTypes, DataType returnType) {
        Class<?> udfClass = udf.getClass();
        Class<?>[] argClasses = new Class<?>[argTypes.length];
        for (int i = 0; i < argTypes.length; i++) {
            argClasses[i] = argTypes[i].getConversionClass();
        }
        Class<?> outClass = returnType.getConversionClass();
        if (argClasses.length == 0) {
            return declineEval(udf, "a zero-argument eval is not supported");
        }
        for (Method method : ExtractionUtils.collectMethods(udfClass, UserDefinedFunctionHelper.SCALAR_EVAL)) {
            if (method.isVarArgs() && ExtractionUtils.isInvokable(method, argClasses)) {
                return declineEval(udf, "a varargs eval is not supported");
            }
        }
        try {
            UserDefinedFunctionHelper.validateClassForRuntime(
                    udfClass.asSubclass(UserDefinedFunction.class),
                    UserDefinedFunctionHelper.SCALAR_EVAL,
                    argClasses,
                    outClass,
                    udfClass.getName());
        } catch (ValidationException e) {
            LOG.debug("Flink rejects UDF {} for these argument types; the call falls back.", udfClass.getName(), e);
            return false;
        }
        return true;
    }

    /**
     * Runs Flink's own instance preparation, which cleans the closure and checks that the instance
     * serializes. It is what turns "this function captures a non-serializable outer reference" from a
     * failure of the running job into a plan-time fallback.
     */
    private static boolean isPreparable(ScalarFunction udf, ConverterContext context) {
        try {
            UserDefinedFunctionHelper.prepareInstance(context.getTableConfig(), udf);
            return true;
        } catch (ValidationException e) {
            LOG.debug(
                    "Cannot prepare UDF instance {}; the call falls back.",
                    udf.getClass().getName(),
                    e);
            return false;
        }
    }

    private static Optional<PhysicalExprNode> decline(ScalarFunction udf, String reason) {
        LOG.debug("Flink UDF {} falls back: {}", udf.getClass().getName(), reason);
        return Optional.empty();
    }

    private static boolean declineEval(ScalarFunction udf, String reason) {
        LOG.debug("Flink UDF {} falls back: {}", udf.getClass().getName(), reason);
        return false;
    }
}
