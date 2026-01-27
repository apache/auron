/*
 * Quick standalone verification that Flink SQL is converted to Auron.
 *
 * Compile and run:
 *   javac -cp "flink-libs/*:auron-libs/*" QuickVerify.java
 *   java -cp ".:flink-libs/*:auron-libs/*" QuickVerify
 */

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class QuickVerify {

    public static void main(String[] args) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("AURON AUTO-CONVERSION QUICK VERIFICATION");
        System.out.println("=".repeat(70) + "\n");

        try {
            // Test 1: Verify configuration works
            System.out.println("Test 1: Checking configuration...");
            Configuration config = new Configuration();
            config.setBoolean("table.optimizer.auron.enabled", true);
            boolean enabled = config.getBoolean("table.optimizer.auron.enabled", false);
            System.out.println("  ✅ Config 'table.optimizer.auron.enabled' = " + enabled);

            // Test 2: Check if Auron processor class exists
            System.out.println("\nTest 2: Checking Flink Auron processor...");
            try {
                Class<?> processorClass = Class.forName(
                    "org.apache.flink.table.planner.plan.nodes.exec.processor.AuronExecNodeGraphProcessor"
                );
                System.out.println("  ✅ AuronExecNodeGraphProcessor class found");
            } catch (ClassNotFoundException e) {
                System.out.println("  ❌ AuronExecNodeGraphProcessor NOT found");
                System.out.println("     Flink needs to be compiled with Auron integration");
            }

            // Test 3: Check if Auron ExecNode exists
            System.out.println("\nTest 3: Checking Auron ExecNode...");
            try {
                Class<?> execNodeClass = Class.forName(
                    "org.apache.flink.table.planner.plan.nodes.exec.batch.AuronBatchExecNode"
                );
                System.out.println("  ✅ AuronBatchExecNode class found");
            } catch (ClassNotFoundException e) {
                System.out.println("  ❌ AuronBatchExecNode NOT found");
            }

            // Test 4: Check if Auron converter exists
            System.out.println("\nTest 4: Checking Auron converter...");
            try {
                Class<?> converterClass = Class.forName(
                    "org.apache.auron.flink.planner.AuronExecNodeConverter"
                );
                System.out.println("  ✅ AuronExecNodeConverter class found");
            } catch (ClassNotFoundException e) {
                System.out.println("  ❌ AuronExecNodeConverter NOT found");
                System.out.println("     Auron JAR needs to be on classpath");
            }

            // Test 5: Compare execution plans
            System.out.println("\nTest 5: Comparing execution plans...");
            System.out.println("  (This is the definitive test)\n");

            // Plan WITHOUT Auron
            System.out.println("  5a. Getting plan WITHOUT Auron...");
            String planWithout = getExecutionPlan(false);

            // Plan WITH Auron
            System.out.println("  5b. Getting plan WITH Auron...");
            String planWith = getExecutionPlan(true);

            // Compare
            System.out.println("\n" + "-".repeat(70));
            System.out.println("EXECUTION PLAN WITHOUT AURON:");
            System.out.println("-".repeat(70));
            printPlanSnippet(planWithout);

            System.out.println("\n" + "-".repeat(70));
            System.out.println("EXECUTION PLAN WITH AURON:");
            System.out.println("-".repeat(70));
            printPlanSnippet(planWith);
            System.out.println("-".repeat(70));

            // Analyze
            System.out.println("\nAnalysis:");
            if (planWithout.equals(planWith)) {
                System.out.println("  ⚠️  Plans are IDENTICAL");
                System.out.println("      Auron conversion is NOT happening");
                System.out.println("      Possible reasons:");
                System.out.println("      - Processor not registered in BatchPlanner");
                System.out.println("      - Auron classes not on classpath");
                System.out.println("      - Pattern not recognized");
            } else {
                System.out.println("  ✅ Plans are DIFFERENT - good!");
            }

            if (planWith.contains("Auron") || planWith.contains("AuronBatchExecNode")) {
                System.out.println("  ✅ 'Auron' found in execution plan");
                System.out.println("  🎉 AUTOMATIC CONVERSION IS WORKING!");
            } else {
                System.out.println("  ❌ 'Auron' NOT found in execution plan");
                System.out.println("      But plans differ, so processor may be running");
                System.out.println("      Check logs for details");
            }

        } catch (Exception e) {
            System.err.println("\n❌ ERROR during verification:");
            e.printStackTrace();
        }

        System.out.println("\n" + "=".repeat(70));
        System.out.println("VERIFICATION COMPLETE");
        System.out.println("=".repeat(70) + "\n");
    }

    private static String getExecutionPlan(boolean auronEnabled) {
        try {
            Configuration config = new Configuration();
            config.setBoolean("table.optimizer.auron.enabled", auronEnabled);
            config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);

            StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

            // Create test table
            tEnv.executeSql(
                "CREATE TABLE test_sales (" +
                "  id BIGINT," +
                "  product STRING," +
                "  amount DOUBLE" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'file:///tmp/sales.parquet'," +
                "  'format' = 'parquet'" +
                ")"
            );

            // Get plan
            return tEnv.explainSql(
                "SELECT id, product FROM test_sales WHERE amount > 100"
            );

        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    private static void printPlanSnippet(String plan) {
        // Find the Optimized Execution Plan section
        int start = plan.indexOf("== Optimized Execution Plan ==");
        if (start == -1) {
            System.out.println(plan.substring(0, Math.min(500, plan.length())));
            return;
        }

        int nextSection = plan.indexOf("==", start + 35);
        if (nextSection == -1) {
            nextSection = plan.length();
        }

        String section = plan.substring(start, nextSection);
        String[] lines = section.split("\n");

        // Print first 10 lines of the execution plan
        for (int i = 0; i < Math.min(10, lines.length); i++) {
            System.out.println(lines[i]);
        }
        if (lines.length > 10) {
            System.out.println("... (" + (lines.length - 10) + " more lines)");
        }
    }
}
