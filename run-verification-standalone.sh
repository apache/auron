#!/bin/bash
# Standalone verification test runner
# This bypasses Maven's POM issues and runs the test directly

set -e

echo "======================================"
echo "Auron Verification Test (Standalone)"
echo "======================================"
echo ""

cd /Users/vsowrira/git/auron

# Compile the test if needed
echo "1. Compiling test class..."
FLINK_CP="/Users/vsowrira/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/flink-table-planner_2.12-1.18-SNAPSHOT.jar"
FLINK_CP="$FLINK_CP:/Users/vsowrira/.m2/repository/org/apache/flink/flink-table-api-java/1.18-SNAPSHOT/flink-table-api-java-1.18-SNAPSHOT.jar"
FLINK_CP="$FLINK_CP:/Users/vsowrira/.m2/repository/org/apache/flink/flink-table-api-java-bridge_2.12/1.18-SNAPSHOT/flink-table-api-java-bridge_2.12-1.18-SNAPSHOT.jar"
FLINK_CP="$FLINK_CP:/Users/vsowrira/.m2/repository/org/apache/flink/flink-streaming-java/1.18-SNAPSHOT/flink-streaming-java-1.18-SNAPSHOT.jar"
FLINK_CP="$FLINK_CP:/Users/vsowrira/.m2/repository/org/apache/flink/flink-core/1.18-SNAPSHOT/flink-core-1.18-SNAPSHOT.jar"
FLINK_CP="$FLINK_CP:/Users/vsowrira/.m2/repository/org/apache/flink/flink-runtime/1.18-SNAPSHOT/flink-runtime-1.18-SNAPSHOT.jar"

# Add JUnit
FLINK_CP="$FLINK_CP:/Users/vsowrira/.m2/repository/org/junit/jupiter/junit-jupiter-api/5.8.1/junit-jupiter-api-5.8.1.jar"
FLINK_CP="$FLINK_CP:/Users/vsowrira/.m2/repository/org/junit/jupiter/junit-jupiter-engine/5.8.1/junit-jupiter-engine-5.8.1.jar"
FLINK_CP="$FLINK_CP:/Users/vsowrira/.m2/repository/org/junit/platform/junit-platform-launcher/1.8.1/junit-platform-launcher-1.8.1.jar"

# Compile test
javac -cp "$FLINK_CP" \
    -d /tmp/auron-test-classes \
    auron-flink-extension/auron-flink-planner/src/test/java/org/apache/auron/flink/planner/AuronAutoConversionVerificationTest.java \
    2>&1 | head -20

if [ $? -eq 0 ]; then
    echo "   ✅ Test compiled"
else
    echo "   ❌ Compilation failed"
    echo "   This is likely due to missing dependencies."
    echo "   Recommendation: Fix the POM spotless issue and build normally."
    exit 1
fi

echo ""
echo "2. Running verification test..."
java -cp "/tmp/auron-test-classes:$FLINK_CP" \
    org.junit.platform.console.ConsoleLauncher \
    --select-class org.apache.auron.flink.planner.AuronAutoConversionVerificationTest \
    --details=verbose

echo ""
echo "======================================"
echo "Test Complete"
echo "======================================"
