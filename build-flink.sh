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

# Simplified build script for Auron with Flink support
# Usage: ./build-flink.sh [clean|test|install]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set Java 17
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Auron Build Script (Flink Profile)${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# Parse command
COMMAND="${1:-install}"
SKIP_TESTS="-DskipTests"

case "$COMMAND" in
  clean)
    echo -e "${GREEN}Running clean build...${NC}"
    PHASE="clean install"
    ;;
  test)
    echo -e "${GREEN}Running build with tests...${NC}"
    PHASE="test"
    SKIP_TESTS=""
    ;;
  install)
    echo -e "${GREEN}Running install (skipping tests)...${NC}"
    PHASE="install"
    ;;
  *)
    echo "Usage: $0 [clean|test|install]"
    echo ""
    echo "Commands:"
    echo "  clean   - Clean build from scratch"
    echo "  test    - Build and run tests"
    echo "  install - Build and install (skip tests, default)"
    exit 1
    ;;
esac

echo -e "${YELLOW}Java:${NC} $JAVA_HOME"
echo -e "${YELLOW}Profiles:${NC} flink-1.18, scala-2.12"
echo -e "${YELLOW}Phase:${NC} $PHASE"
echo ""

cd "$SCRIPT_DIR"

# Build only Flink modules (avoid Spark compilation issues)
./build/apache-maven-3.9.12/bin/mvn $PHASE $SKIP_TESTS \
  -pl auron-flink-extension/auron-flink-planner -am \
  -Pflink-1.18 -Pscala-2.12

echo ""
echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}Build completed successfully!${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""
echo "Next steps:"
echo "  • Run examples: cd auron-flink-extension/auron-flink-planner && ./run-example.sh"
echo "  • Run specific test: ./run-example.sh [mvp|parallel|groupby]"
echo ""
