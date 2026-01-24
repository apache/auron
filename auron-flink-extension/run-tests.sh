#!/bin/bash

# Test runner script for Auron-Flink Integration
# This script runs all unit and integration tests

set -e  # Exit on error

echo "╔════════════════════════════════════════════════════════╗"
echo "║  Auron-Flink Integration Test Suite                   ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

cd "$(dirname "$0")/.."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "📋 Test Plan:"
echo "  1. Unit Tests (Type Converter, Expression Converter, Converters, Adaptor)"
echo "  2. Integration Tests (Parquet Scan)"
echo ""

# Check if native library exists
if [ -f "native-engine/target/release/libauron.dylib" ] || [ -f "native-engine/target/release/libauron.so" ]; then
    echo "✓ Native library found - integration tests will run"
    RUN_INTEGRATION=true
else
    echo "⚠️  Native library NOT found - integration tests will be skipped"
    echo "   Build with: cd native-engine && cargo build --release"
    RUN_INTEGRATION=false
fi

echo ""
echo "════════════════════════════════════════════════════════"
echo "Phase 1: Unit Tests"
echo "════════════════════════════════════════════════════════"

# Run unit tests
./build/mvn test \
  -pl auron-flink-extension/auron-flink-runtime,auron-flink-extension/auron-flink-planner \
  -Dtest='*Test'

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Unit tests passed${NC}"
else
    echo -e "${RED}✗ Unit tests failed${NC}"
    exit 1
fi

echo ""
echo "════════════════════════════════════════════════════════"
echo "Phase 2: Integration Tests"
echo "════════════════════════════════════════════════════════"

if [ "$RUN_INTEGRATION" = true ]; then
    # Run integration tests
    ./build/mvn test \
      -pl auron-flink-extension/auron-flink-planner \
      -Dtest='*ITCase'

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Integration tests passed${NC}"
    else
        echo -e "${RED}✗ Integration tests failed${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}⏭️  Skipping integration tests (native library not available)${NC}"
fi

echo ""
echo "╔════════════════════════════════════════════════════════╗"
echo "║  ✅ All Tests Completed Successfully                  ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

echo "📊 Test Summary:"
echo "  • FlinkTypeConverterTest: Type conversions"
echo "  • FlinkExpressionConverterTest: Expression conversions"
echo "  • AuronFlinkConvertersTest: Plan conversions"
echo "  • FlinkAuronAdaptorTest: Adaptor and SPI loading"
if [ "$RUN_INTEGRATION" = true ]; then
    echo "  • AuronFlinkParquetScanITCase: End-to-end Parquet queries"
fi
echo ""

echo "Next steps:"
echo "  • Review test results above"
echo "  • Check target/surefire-reports/ for detailed reports"
echo "  • Run specific tests with: ./build/mvn test -Dtest=ClassName"
