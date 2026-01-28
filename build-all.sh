#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Unified build script for Flink + Auron integration
# This script builds both Flink (with Auron integration) and Auron itself
#
# Usage: ./build-all.sh [OPTIONS]
#
# Options:
#   --flink-only        Build only Flink
#   --auron-only        Build only Auron (assumes Flink is already built)
#   --clean             Clean build from scratch for both
#   --flink-clean       Clean Flink build only
#   --auron-clean       Clean Auron build only
#   --skip-tests        Skip tests for both builds (faster)
#   --help              Show this help message

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINK_DIR="/Users/vsowrira/git/flink"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default options
BUILD_FLINK=true
BUILD_AURON=true
FLINK_CLEAN=""
AURON_CLEAN=""
SKIP_TESTS="-DskipTests"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --flink-only)
      BUILD_AURON=false
      shift
      ;;
    --auron-only)
      BUILD_FLINK=false
      shift
      ;;
    --clean)
      FLINK_CLEAN="clean"
      AURON_CLEAN="clean"
      shift
      ;;
    --flink-clean)
      FLINK_CLEAN="clean"
      shift
      ;;
    --auron-clean)
      AURON_CLEAN="clean"
      shift
      ;;
    --skip-tests)
      SKIP_TESTS="-DskipTests"
      shift
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --flink-only        Build only Flink"
      echo "  --auron-only        Build only Auron (assumes Flink is already built)"
      echo "  --clean             Clean build from scratch for both"
      echo "  --flink-clean       Clean Flink build only"
      echo "  --auron-clean       Clean Auron build only"
      echo "  --skip-tests        Skip tests for both builds (faster)"
      echo "  --help              Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0                          # Build both Flink and Auron"
      echo "  $0 --clean                  # Clean build both from scratch"
      echo "  $0 --flink-only             # Build only Flink"
      echo "  $0 --auron-only             # Build only Auron"
      echo "  $0 --skip-tests             # Fast build, skip all tests"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Run '$0 --help' for usage"
      exit 1
      ;;
  esac
done

# Set Java 17
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}Flink + Auron Unified Build Script${NC}"
echo -e "${BLUE}======================================================================${NC}"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo -e "  Java: $JAVA_HOME"
echo -e "  Build Flink: $BUILD_FLINK"
echo -e "  Build Auron: $BUILD_AURON"
echo -e "  Skip Tests: $([ \"$SKIP_TESTS\" = \"-DskipTests\" ] && echo \"Yes\" || echo \"No\")"
echo ""

# Function to check if Flink needs to be built
check_flink_build_needed() {
  FLINK_JAR="$HOME/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/flink-table-planner_2.12-1.18-SNAPSHOT.jar"

  if [ ! -f "$FLINK_JAR" ]; then
    echo -e "${YELLOW}Flink 1.18-SNAPSHOT JAR not found in Maven local repository${NC}"
    return 0  # Need to build
  fi

  # Check if JAR contains Auron integration classes
  if jar tf "$FLINK_JAR" | grep -q "AuronExecNodeGraphProcessor"; then
    echo -e "${GREEN}Flink 1.18-SNAPSHOT with Auron integration already built${NC}"
    return 1  # No need to build
  else
    echo -e "${YELLOW}Flink 1.18-SNAPSHOT found but missing Auron integration classes${NC}"
    return 0  # Need to build
  fi
}

# Build Flink
if [ "$BUILD_FLINK" = true ]; then
  echo -e "${BLUE}======================================================================${NC}"
  echo -e "${BLUE}Step 1: Building Flink 1.18 with Auron Integration${NC}"
  echo -e "${BLUE}======================================================================${NC}"
  echo ""

  if [ -z "$FLINK_CLEAN" ] && [ "$BUILD_FLINK" = true ]; then
    if check_flink_build_needed; then
      echo -e "${YELLOW}Building Flink...${NC}"
    else
      echo -e "${GREEN}Skipping Flink build (already up-to-date)${NC}"
      echo -e "${YELLOW}Use --flink-clean to force rebuild${NC}"
      echo ""
      BUILD_FLINK=false
    fi
  fi

  if [ "$BUILD_FLINK" = true ]; then
    if [ ! -d "$FLINK_DIR" ]; then
      echo -e "${RED}ERROR: Flink directory not found: $FLINK_DIR${NC}"
      exit 1
    fi

    cd "$FLINK_DIR"

    # Check branch
    CURRENT_BRANCH=$(git branch --show-current)
    if [ "$CURRENT_BRANCH" != "auron-flink-1.18-integration" ]; then
      echo -e "${YELLOW}WARNING: Current branch is '$CURRENT_BRANCH', expected 'auron-flink-1.18-integration'${NC}"
      echo -e "${YELLOW}Switch branch with: cd $FLINK_DIR && git checkout auron-flink-1.18-integration${NC}"
      exit 1
    fi

    echo -e "${GREEN}Building Flink 1.18-SNAPSHOT (branch: $CURRENT_BRANCH)...${NC}"
    echo -e "${YELLOW}This may take 10-15 minutes for the first build...${NC}"
    echo ""

    # Build only the modules we need for Auron integration
    # flink-table-api-java: Contains OptimizerConfigOptions
    # flink-table-planner: Contains AuronExecNodeGraphProcessor and AuronBatchExecNode
    ./mvnw $FLINK_CLEAN install $SKIP_TESTS \
      -pl flink-table/flink-table-api-java,flink-table/flink-table-planner \
      -am \
      -Pscala-2.12

    echo ""
    echo -e "${GREEN}✅ Flink build completed successfully!${NC}"

    # Verify build
    echo ""
    echo -e "${BLUE}Verifying Flink build...${NC}"
    FLINK_PLANNER_JAR="$HOME/.m2/repository/org/apache/flink/flink-table-planner_2.12/1.18-SNAPSHOT/flink-table-planner_2.12-1.18-SNAPSHOT.jar"

    if jar tf "$FLINK_PLANNER_JAR" | grep -q "AuronExecNodeGraphProcessor"; then
      echo -e "${GREEN}✅ Auron integration classes found in Flink JAR${NC}"
    else
      echo -e "${RED}❌ ERROR: Auron integration classes NOT found in Flink JAR${NC}"
      exit 1
    fi

    echo ""
  fi
fi

# Build Auron
if [ "$BUILD_AURON" = true ]; then
  echo -e "${BLUE}======================================================================${NC}"
  echo -e "${BLUE}Step 2: Building Auron with Flink Support${NC}"
  echo -e "${BLUE}======================================================================${NC}"
  echo ""

  cd "$SCRIPT_DIR"

  echo -e "${GREEN}Building Auron against Flink 1.18-SNAPSHOT...${NC}"
  echo ""

  # Use the existing build-flink.sh script
  if [ -n "$AURON_CLEAN" ]; then
    ./build-flink.sh clean
  else
    ./build-flink.sh install
  fi

  echo ""
  echo -e "${GREEN}✅ Auron build completed successfully!${NC}"
  echo ""
fi

# Summary
echo -e "${BLUE}======================================================================${NC}"
echo -e "${GREEN}✅ Build Completed Successfully!${NC}"
echo -e "${BLUE}======================================================================${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo ""

if [ "$BUILD_FLINK" = true ] && [ "$BUILD_AURON" = true ]; then
  echo "  • Run examples:"
  echo "    cd auron-flink-extension/auron-flink-planner"
  echo "    ./run-example.sh groupby    # Hybrid execution demo"
  echo "    ./run-example.sh parallel   # Parallel execution test"
  echo ""
  echo "  • Run integration tests:"
  echo "    cd auron-flink-extension/auron-flink-planner"
  echo "    ./run-e2e-test-final.sh"
elif [ "$BUILD_FLINK" = true ]; then
  echo "  • Flink 1.18-SNAPSHOT with Auron integration is now in Maven local"
  echo "  • Build Auron with: ./build-all.sh --auron-only"
elif [ "$BUILD_AURON" = true ]; then
  echo "  • Auron with Flink support is ready"
  echo "  • Run examples: cd auron-flink-extension/auron-flink-planner && ./run-example.sh"
fi

echo ""
