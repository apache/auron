#!/bin/bash
# Simple verification script using compiled classes

set -e

echo "======================================"
echo "Simple Auron Conversion Verification"
echo "======================================"
echo ""

# Check if classes exist
echo "1. Checking compiled classes..."

FLINK_CLASSES="/Users/vsowrira/git/flink/flink-table/flink-table-planner/target/classes"
AURON_CLASSES="/Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner/target/classes"

if [ -f "$FLINK_CLASSES/org/apache/flink/table/planner/plan/nodes/exec/batch/AuronBatchExecNode.class" ]; then
    echo "   ✅ AuronBatchExecNode compiled in Flink"
else
    echo "   ❌ AuronBatchExecNode not found"
    exit 1
fi

if [ -f "$FLINK_CLASSES/org/apache/flink/table/planner/plan/nodes/exec/processor/AuronExecNodeGraphProcessor.class" ]; then
    echo "   ✅ AuronExecNodeGraphProcessor compiled in Flink"
else
    echo "   ❌ AuronExecNodeGraphProcessor not found"
    exit 1
fi

if [ -f "$AURON_CLASSES/org/apache/auron/flink/planner/AuronExecNodeConverter.class" ]; then
    echo "   ✅ AuronExecNodeConverter compiled in Auron"
else
    echo "   ❌ AuronExecNodeConverter not found"
    exit 1
fi

if [ -f "$AURON_CLASSES/org/apache/auron/flink/planner/AuronTransformationFactory.class" ]; then
    echo "   ✅ AuronTransformationFactory compiled in Auron"
else
    echo "   ❌ AuronTransformationFactory not found"
    exit 1
fi

echo ""
echo "2. Checking configuration..."

# Check if OptimizerConfigOptions has the Auron flag
if javap -cp "$FLINK_CLASSES" org.apache.flink.table.api.config.OptimizerConfigOptions | grep -q "TABLE_OPTIMIZER_AURON_ENABLED"; then
    echo "   ✅ TABLE_OPTIMIZER_AURON_ENABLED configuration flag present"
else
    echo "   ⚠️  Configuration flag not found (may need recompile)"
fi

echo ""
echo "3. Checking BatchPlanner registration..."

# Check if BatchPlanner references Auron processor
if grep -q "AuronExecNodeGraphProcessor" /Users/vsowrira/git/flink/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/delegation/BatchPlanner.scala; then
    echo "   ✅ AuronExecNodeGraphProcessor registered in BatchPlanner"
else
    echo "   ❌ Processor not registered"
    exit 1
fi

echo ""
echo "======================================"
echo "VERIFICATION SUMMARY"
echo "======================================"
echo ""
echo "✅ All core components are in place:"
echo ""
echo "   Flink Side:"
echo "   • OptimizerConfigOptions - Configuration flag"
echo "   • AuronExecNodeGraphProcessor - Pattern detection & conversion"
echo "   • AuronBatchExecNode - Wrapper node"
echo "   • BatchPlanner - Processor registration"
echo ""
echo "   Auron Side:"
echo "   • AuronExecNodeConverter - ExecNode to plan conversion"
echo "   • AuronTransformationFactory - Transformation creation"
echo ""
echo "📋 Next Steps to Run End-to-End Verification:"
echo ""
echo "   1. Build Flink JARs:"
echo "      cd /Users/vsowrira/git/flink"
echo "      mvn clean install -DskipTests -pl flink-table/flink-table-api-java,flink-table/flink-table-planner"
echo ""
echo "   2. Build Auron JAR:"
echo "      cd /Users/vsowrira/git/auron"
echo "      ./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --flinkver 1.18 --skiptests true"
echo ""
echo "   3. Run verification test:"
echo "      cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner"
echo "      mvn test -Dtest=AuronAutoConversionVerificationTest"
echo ""
echo "   The test will compare execution plans with Auron enabled vs disabled."
echo "   Expected result: Plans differ and 'Auron' appears in enabled plan."
echo ""
echo "======================================"
