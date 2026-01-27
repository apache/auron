#!/bin/bash
# Script to verify that Flink SQL queries are automatically converted to Auron native execution

set -e

echo "========================================"
echo "Auron Auto-Conversion Verification"
echo "========================================"
echo ""

# Check Java version
echo "1. Checking Java version..."
java -version 2>&1 | head -1
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
echo "   Using JAVA_HOME: $JAVA_HOME"
echo ""

# Check if Flink planner was compiled with Auron support
echo "2. Checking Flink compilation..."
FLINK_PLANNER_JAR="/Users/vsowrira/git/flink/flink-table/flink-table-planner/target/flink-table-planner_2.12-2.3-SNAPSHOT.jar"
if [ -f "$FLINK_PLANNER_JAR" ]; then
    echo "   ✅ Flink table planner JAR exists"

    # Check if AuronExecNodeGraphProcessor is in the JAR
    if jar tf "$FLINK_PLANNER_JAR" | grep -q "AuronExecNodeGraphProcessor"; then
        echo "   ✅ AuronExecNodeGraphProcessor found in Flink JAR"
    else
        echo "   ❌ AuronExecNodeGraphProcessor NOT found in Flink JAR"
        echo "      You need to recompile Flink with the Auron integration"
    fi

    # Check if AuronBatchExecNode is in the JAR
    if jar tf "$FLINK_PLANNER_JAR" | grep -q "AuronBatchExecNode"; then
        echo "   ✅ AuronBatchExecNode found in Flink JAR"
    else
        echo "   ❌ AuronBatchExecNode NOT found in Flink JAR"
    fi
else
    echo "   ⚠️  Flink table planner JAR not found at expected location"
    echo "      Expected: $FLINK_PLANNER_JAR"
fi
echo ""

# Check if Auron Flink extension was compiled
echo "3. Checking Auron compilation..."
AURON_JAR="./auron-flink-extension/auron-flink-planner/target/auron-flink-planner-7.0.0-SNAPSHOT.jar"
if [ -f "$AURON_JAR" ]; then
    echo "   ✅ Auron Flink planner JAR exists"
    JAR_SIZE=$(ls -lh "$AURON_JAR" | awk '{print $5}')
    echo "      Size: $JAR_SIZE"

    # Check if converter classes are in the JAR
    if jar tf "$AURON_JAR" | grep -q "AuronExecNodeConverter"; then
        echo "   ✅ AuronExecNodeConverter found in Auron JAR"
    else
        echo "   ❌ AuronExecNodeConverter NOT found"
    fi

    if jar tf "$AURON_JAR" | grep -q "AuronTransformationFactory"; then
        echo "   ✅ AuronTransformationFactory found in Auron JAR"
    else
        echo "   ❌ AuronTransformationFactory NOT found"
    fi
else
    echo "   ❌ Auron Flink planner JAR not found"
    echo "      Expected: $AURON_JAR"
    echo "      Run: ./auron-build.sh --pre --sparkver 3.5 --scalaver 2.12 --flinkver 1.18 --skiptests true"
fi
echo ""

# Run the verification test
echo "4. Running verification test..."
echo "   This will check if execution plans differ with Auron enabled vs disabled"
echo ""

cd auron-flink-extension/auron-flink-planner

# Run just the verification test
export MAVEN_OPTS="-Xmx2g"
../../build/apache-maven-3.9.12/bin/mvn test \
    -Dtest=AuronAutoConversionVerificationTest \
    -DfailIfNoTests=false \
    2>&1 | tee /tmp/auron-verification.log

echo ""
echo "========================================"
echo "Verification Complete"
echo "========================================"
echo ""
echo "Check the output above for:"
echo "  ✅ 'Plans are different' - Auron is being invoked"
echo "  ✅ 'Auron appears in execution plan' - Full conversion working"
echo "  ⚠️  'Plans are identical' - Need to investigate"
echo ""
echo "Full log saved to: /tmp/auron-verification.log"
echo ""
echo "To see detailed execution plan:"
echo "  grep -A 30 'PLAN WITH AURON' /tmp/auron-verification.log"
echo ""
