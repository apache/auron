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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.auron.flink.functions.AuronGeneratedUDF;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ExprCodeGenerator;
import org.apache.flink.table.planner.codegen.GeneratedExpression;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates the class that invokes a user-defined scalar function, using Flink's own expression
 * generator.
 *
 * <p>Flink resolves the {@code eval} overload, the argument conversions and the result conversion
 * while generating the call, which is the same resolution the query would get without Auron. The
 * only thing written here is the class the generated expression is placed in: Flink's own class
 * assembler supports a fixed set of target interfaces and this is not one of them, so the template
 * below is modelled on the standalone class Flink emits for its expression evaluator.
 *
 * <p>The call's operands are rewritten to input references into a row holding one field per
 * argument, because the generated class receives its arguments as a row rather than as the columns
 * of the surrounding Calc's input. The operands' own types are carried through unchanged, so the
 * conversions Flink derives are the ones the untouched call would get.
 *
 * <p><b>The classloader passed in must be able to see the user function class.</b> It is used
 * twice: the generator deserializes its own copy of the function through it, and it becomes
 * Janino's parent loader when the emitted source is compiled. A loader that cannot see the
 * function fails both, and both failures are declines — indistinguishable at the call site from
 * an ordinary policy decline such as an unsupported argument type. The visible symptom is therefore
 * not an error but the feature quietly doing nothing: every user function call falls back to
 * Flink's own Calc, correct but unaccelerated. The reason for the decline is on this class's
 * logger at debug.
 */
public final class FlinkUDFCodeGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUDFCodeGenerator.class);

    private FlinkUDFCodeGenerator() {}

    /**
     * The generated source together with the reference array it can only be instantiated against.
     */
    public static final class GeneratedCode {

        private final String className;
        private final String code;
        private final Object[] references;

        private GeneratedCode(String className, String code, Object[] references) {
            this.className = className;
            this.code = code;
            this.references = references;
        }

        /**
         * Returns the name the generated class was emitted under.
         *
         * @return the class name
         */
        public String getClassName() {
            return className;
        }

        /**
         * Returns the generated Java source.
         *
         * @return the source
         */
        public String getCode() {
            return code;
        }

        /**
         * Returns the argument the generated class's single {@code (Object[])} constructor takes.
         *
         * <p>The array itself, not a copy. Its entries are the live user function instance and its
         * converters, so copying the array would protect nothing that matters while suggesting it
         * did. Callers pass it straight into the payload.
         *
         * @return the reference array
         */
        public Object[] getReferences() {
            return references;
        }
    }

    /**
     * Generates and validates the invoker for one user function call, or returns empty if it cannot
     * be generated.
     *
     * <p>The generated source is compiled here and the result discarded. A source that does not
     * compile is a plan-time decline, which costs a fallback to Flink's generated Calc; the same
     * source reaching the task instead fails the running job with a message naming a synthetic
     * class. The client holds the user function on its classpath, so the check is faithful, and
     * {@code CompileUtils} keys its cache on the classloader and the source rather than the class
     * name, so nothing compiled here can be mistaken for the task's own compilation.
     *
     * @param call the user function call to generate an invoker for
     * @param paramsRowType one field per {@code eval} argument, in argument order
     * @param tableConfig the table configuration the generator reads its code-generation settings
     *     from
     * @param classLoader the classloader that can see the user function class
     * @return the generated source and its references, or empty if generation or compilation fails
     */
    public static Optional<GeneratedCode> generate(
            RexCall call, RowType paramsRowType, ReadableConfig tableConfig, ClassLoader classLoader) {
        String className = CodeGenUtils.newName("AuronGeneratedUDF");
        String code;
        Object[] references;
        try {
            CodeGeneratorContext ctx = new CodeGeneratorContext(tableConfig, classLoader);
            ExprCodeGenerator exprGen = new ExprCodeGenerator(ctx, false);
            exprGen.bindInput(paramsRowType, CodeGenUtils.DEFAULT_INPUT1_TERM(), scala.Option.empty());
            GeneratedExpression expr = exprGen.generateExpression(rewriteOperandsToInputRefs(call));
            code = emitClass(className, ctx, expr);
            references = collectReferences(ctx);
        } catch (Exception e) {
            LOG.debug("Cannot generate an invoker for Flink UDF {}; the call falls back.", call.getOperator(), e);
            return Optional.empty();
        }

        try {
            CompileUtils.compile(classLoader, className, code);
        } catch (Exception e) {
            // Janino's failure path writes the offending source to stdout, where a planning client
            // is unlikely to look. Repeat it on the logger the rest of the decline path uses.
            LOG.debug(
                    "Generated invoker for Flink UDF {} does not compile; the call falls back. Source:\n{}",
                    call.getOperator(),
                    code,
                    e);
            return Optional.empty();
        }
        return Optional.of(new GeneratedCode(className, code, references));
    }

    /**
     * Rebuilds the call over references into the arguments row, keeping each operand's own type so
     * the conversions Flink derives are unchanged.
     */
    private static RexNode rewriteOperandsToInputRefs(RexCall call) {
        List<RexNode> refs = new ArrayList<>(call.getOperands().size());
        for (int i = 0; i < call.getOperands().size(); i++) {
            refs.add(new RexInputRef(i, call.getOperands().get(i).getType()));
        }
        return call.clone(call.getType(), refs);
    }

    private static String emitClass(String className, CodeGeneratorContext ctx, GeneratedExpression expr) {
        String inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM();
        return "public final class " + className + " implements " + AuronGeneratedUDF.class.getName() + " {\n"
                + "  private final Object[] references;\n"
                + "  private transient org.apache.flink.api.common.functions.RuntimeContext runtimeContext;\n"
                + ctx.reuseMemberCode() + "\n"
                + ctx.reuseInnerClassDefinitionCode() + "\n"
                + "  public " + className + "(Object[] references) throws Exception {\n"
                + "    this.references = references;\n"
                + ctx.reuseInitCode() + "\n"
                + "  }\n"
                // Whole constructor declarations, not statements, so they sit beside the one above
                // rather than inside it. Empty in practice: nothing in the planner registers a
                // reusable constructor, and the declarations it would emit open with a this() call
                // this class has no no-argument constructor to satisfy.
                + ctx.reuseConstructorCode(className) + "\n"
                // Generated open and converter statements reach the runtime context through a
                // getter of exactly this name, which is what a Flink rich function would expose.
                + "  public org.apache.flink.api.common.functions.RuntimeContext getRuntimeContext() {\n"
                + "    return runtimeContext;\n"
                + "  }\n"
                + "  public void open(org.apache.flink.api.common.functions.RuntimeContext rc) throws Exception {\n"
                + "    this.runtimeContext = rc;\n"
                + ctx.reuseOpenCode() + "\n"
                + "  }\n"
                + "  public Object eval(org.apache.flink.table.data.RowData " + inputTerm + ") throws Exception {\n"
                + ctx.reuseLocalVariableCode(ctx.reuseLocalVariableCode$default$1()) + "\n"
                + ctx.reuseInputUnboxingCode() + "\n"
                + expr.code() + "\n"
                + "    if (" + expr.nullTerm() + ") {\n"
                + "      return null;\n"
                + "    }\n"
                + "    " + CodeGenUtils.boxedTypeTermForType(expr.resultType()) + " result$ = " + expr.resultTerm()
                + ";\n"
                + "    return result$;\n"
                + "  }\n"
                + "  public void close() throws Exception {\n"
                + ctx.reuseCloseCode() + "\n"
                + "  }\n"
                + "}\n";
    }

    private static Object[] collectReferences(CodeGeneratorContext ctx) {
        scala.collection.mutable.ArrayBuffer<Object> buffer = ctx.references();
        Object[] references = new Object[buffer.size()];
        for (int i = 0; i < references.length; i++) {
            references[i] = buffer.apply(i);
        }
        return references;
    }
}
