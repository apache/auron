#!/bin/bash

# Simplified script to run Auron-Flink examples
# Usage: ./run-example.sh [example-name]
#   example-name: mvp | parallel | groupby

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AURON_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Set Java 17
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Auron-Flink Example Runner${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Check which example to run
EXAMPLE="${1:-groupby}"

case "$EXAMPLE" in
  mvp)
    CLASS_NAME="org.apache.auron.flink.examples.AuronFlinkMVPWorkingExample"
    DESCRIPTION="MVP Working Example (parallelism=1)"
    ;;
  parallel)
    CLASS_NAME="org.apache.auron.flink.examples.AuronFlinkParallelTest"
    DESCRIPTION="Parallel Test (50K rows, parallelism=4)"
    ;;
  groupby)
    CLASS_NAME="org.apache.auron.flink.examples.AuronFlinkGroupByTest"
    DESCRIPTION="GROUP BY Hybrid Execution (10K rows, parallelism=4)"
    ;;
  *)
    echo "Usage: $0 [mvp|parallel|groupby]"
    echo ""
    echo "Examples:"
    echo "  mvp      - MVP Working Example (parallelism=1)"
    echo "  parallel - Parallel Test (50K rows, parallelism=4)"
    echo "  groupby  - GROUP BY Hybrid Execution (default)"
    exit 1
    ;;
esac

echo -e "${GREEN}Running: ${DESCRIPTION}${NC}"
echo ""

# Check if dependencies are available
if [ ! -d "$SCRIPT_DIR/target/lib" ]; then
  echo "Dependencies not found. Copying dependencies..."
  cd "$AURON_ROOT"
  ./build/apache-maven-3.9.12/bin/mvn dependency:copy-dependencies \
    -pl auron-flink-extension/auron-flink-planner -am \
    -DoutputDirectory=target/lib -DincludeScope=test \
    -Pflink-1.18 -Pscala-2.12 -q
  echo ""
fi

# Check if test classes are compiled
TEST_CLASS_FILE="$SCRIPT_DIR/target/test-classes/org/apache/auron/flink/examples/AuronFlinkMVPWorkingExample.class"
if [ ! -f "$TEST_CLASS_FILE" ]; then
  echo "Test classes not compiled. Compiling..."
  cd "$AURON_ROOT"
  ./build/apache-maven-3.9.12/bin/mvn test-compile \
    -pl auron-flink-extension/auron-flink-planner -am \
    -Pflink-1.18 -Pscala-2.12 -DskipBuildNative=true -q
  echo ""
fi

# Run the example
CLASSPATH="$SCRIPT_DIR/target/test-classes:$SCRIPT_DIR/target/classes:$SCRIPT_DIR/target/lib/*"

$JAVA_HOME/bin/java \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  -cp "$CLASSPATH" \
  "$CLASS_NAME"
