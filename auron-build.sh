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

# Function to display script usage
print_help() {
    echo "Usage: $0 [OPTIONS] <maven build options>"
    echo "Build Auron project with specified Maven profiles"
    echo
    echo "Options:"
    echo "  --docker <true|false>    Build in Docker environment (default: false)"
    echo "  --image <NAME>           Docker image to use (centos7|ubuntu24|rockylinux8|debian11, default: centos7)"
    echo "  --pre                    Activate pre-release profile"
    echo "  --release                Activate release profile"
    echo "  --sparkver <VERSION>     Specify Spark version (e.g. 3.0/3.1/3.2/3.3/3.4/3.5)"
    echo "  --scalaver <VERSION>     Specify Scala version (e.g. 2.12/2.13)"
    echo "  --celeborn <VERSION>     Specify Celeborn version (e.g. 0.5/0.6)"
    echo "  --uniffle <VERSION>      Specify Uniffle version (e.g. 0.10)"
    echo "  --paimon <VERSION>       Specify Paimon version (e.g. 1.2)"
    echo "  --clean <true|false>     Clean before build (default: true)"
    echo "  --skiptests <true|false> Skip unit tests (default: true)"
    echo "  --flink <VERSION>        Specify Flink version (e.g. 1.18)"
    echo "  -h, --help               Show this help message"
    echo
    echo "Examples:"
    echo "  $0 --pre --sparkver 3.5 --scalaver 2.12 -DskipBuildNative"
    echo "  $0 --docker true --image centos7 --clean true --skiptests true --release --sparkver 3.5 --scalaver 2.12 --celeborn 0.5 --uniffle 0.10 --paimon 1.2"
    exit 0
}

MVN_CMD="$(dirname "$0")/build/mvn"

# Initialize variables
USE_DOCKER=false
IMAGE_NAME="centos7"
PRE_PROFILE=false
RELEASE_PROFILE=false
CLEAN=true
SKIP_TESTS=true
SPARK_VER=""
SCALA_VER=""
CELEBORN_VER=""
UNIFFLE_VER=""
PAIMON_VER=""
FLINK_VER=""

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --pre)
            PRE_PROFILE=true
            shift
            ;;
        --release)
            RELEASE_PROFILE=true
            shift
            ;;
        --docker)
            if [[ -n "$2" && "$2" =~ ^(true|false)$ ]]; then
                USE_DOCKER="$2"
                shift 2
            else
                echo "ERROR: --docker requires true/false" >&2
                exit 1
            fi
            ;;
        --image)
            if [[ -n "$2" && "$2" != -* ]]; then
                IMAGE_NAME="$2"
                if [[ ! "$IMAGE_NAME" =~ ^(centos7|ubuntu24|rockylinux8|debian11)$ ]]; then
                    echo "ERROR: Unsupported image '$IMAGE_NAME'. Supported: centos7, ubuntu24, rockylinux8, debian11" >&2
                    exit 1
                fi
                shift 2
            else
                echo "ERROR: --image requires one of: centos7, ubuntu24, rockylinux8, debian11" >&2
                exit 1
            fi
            ;;
        --clean)
            if [[ -n "$2" && "$2" =~ ^(true|false)$ ]]; then
                CLEAN="$2"
                shift 2
            else
                echo "ERROR: --clean requires true/false" >&2
                exit 1
            fi
            ;;
        --skiptests)
            if [[ -n "$2" && "$2" =~ ^(true|false)$ ]]; then
                SKIP_TESTS="$2"
                shift 2
            else
                echo "ERROR: --skiptests requires true/false" >&2
                exit 1
            fi
            ;;
        --sparkver)
            if [[ -n "$2" && "$2" != -* ]]; then
                SPARK_VER="$2"
                if [ "$SPARK_VER" = "3.0" ] || [ "$SPARK_VER" = "3.1" ] \
                  || [ "$SPARK_VER" = "3.2" ] || [ "$SPARK_VER" = "3.3" ] \
                  || [ "$SPARK_VER" = "3.4" ] || [ "$SPARK_VER" = "3.5" ]; then
                  echo "Building for Spark $SPARK_VER"
                else
                  echo "ERROR: Invalid Spark version: $SPARK_VER. The currently supported versions are: 3.0 / 3.1 / 3.2 / 3.3 / 3.4 / 3.5."
                  exit 1
                fi
                shift 2
            else
                echo "ERROR: --sparkver requires version argument" >&2
                exit 1
            fi
            ;;
        --scalaver)
            if [[ -n "$2" && "$2" != -* ]]; then
                SCALA_VER="$2"
                if [ "$SCALA_VER" = "2.12" ] || [ "$SCALA_VER" = "2.13" ]; then
                  echo "Building scala version: $SCALA_VER"
                else
                  echo "ERROR: Invalid scala version: $SCALA_VER. The currently supported versions are: 2.12 / 2.13."
                  exit 1
                fi
                shift 2
            else
                echo "ERROR: --scalaver requires version argument" >&2
                exit 1
            fi
            ;;
        --celeborn)
            if [[ -n "$2" && "$2" != -* ]]; then
                CELEBORN_VER="$2"
                if [ "$CELEBORN_VER" = "0.5" ] || [ "$CELEBORN_VER" = "0.6" ]; then
                  echo "Building Celeborn version: $CELEBORN_VER"
                else
                  echo "ERROR: Invalid Celeborn version: $CELEBORN_VER. The currently supported versions are: 0.5 / 0.6."
                  exit 1
                fi
                shift 2
            else
                echo "ERROR: --celeborn requires version argument" >&2
                exit 1
            fi
            ;;
        --uniffle)
            if [[ -n "$2" && "$2" != -* ]]; then
                UNIFFLE_VER="$2"
                if [ "$UNIFFLE_VER" = "0.10" ] ; then
                  echo "Building Uniffle version: $UNIFFLE_VER"
                else
                  echo "ERROR: Invalid Uniffle version: $UNIFFLE_VER. The currently supported version is: 0.10."
                  exit 1
                fi
                shift 2
            else
                echo "ERROR: --uniffle requires version argument" >&2
                exit 1
            fi
            ;;
        --paimon)
            if [[ -n "$2" && "$2" != -* ]]; then
                PAIMON_VER="$2"
                if [ "$PAIMON_VER" = "1.2" ] ; then
                  echo "Building Paimon version: $PAIMON_VER"
                else
                  echo "ERROR: Invalid Paimon version: $PAIMON_VER. The currently supported version is: 1.2."
                  exit 1
                fi
                shift 2
            else
                echo "ERROR: --paimon requires version argument" >&2
                exit 1
            fi
            ;;
        --flink)
            if [[ -n "$2" && "$2" != -* ]]; then
                FLINK_VER="$2"
                if [ "$FLINK_VER" = "1.18" ] ; then
                  echo "Building Flink version: $FLINK_VER"
                else
                  echo "ERROR: Invalid Flink version: $FLINK_VER. The currently supported version is: 1.18"
                  exit 1
                fi
                shift 2
            else
                echo "ERROR: --flink requires version argument" >&2
                exit 1
            fi
            ;;
        -h|--help)
            print_help
            ;;
        --*)
            echo "ERROR: Unknown option '$1'" >&2
            echo "Use '$0 --help' for usage information" >&2
            exit 1
            ;;
        -*)
            break
            ;;
        *)
            echo "ERROR: $1 is not supported" >&2
            echo "Use '$0 --help' for usage information" >&2
            exit 1
            ;;
    esac
done

# Validate required options
MISSING_REQUIREMENTS=()
if [[ "$PRE_PROFILE" == false && "$RELEASE_PROFILE" == false ]]; then
    MISSING_REQUIREMENTS+=("--pre or --release must be specified")
fi
if [[ -z "$SPARK_VER" ]]; then
    MISSING_REQUIREMENTS+=("--sparkver must be specified")
fi
if [[ -z "$SCALA_VER" ]]; then
    MISSING_REQUIREMENTS+=("--scalaver must be specified")
fi

if [[ "${#MISSING_REQUIREMENTS[@]}" -gt 0 ]]; then
    echo "ERROR: Missing required arguments:" >&2
    for req in "${MISSING_REQUIREMENTS[@]}"; do
        echo "  * $req" >&2
    done
    echo
    echo "Use '$0 --help' for usage information" >&2
    exit 1
fi

if [[ "$PRE_PROFILE" == true && "$RELEASE_PROFILE" == true ]]; then
    echo "ERROR: Cannot use both --pre and --release simultaneously" >&2
    exit 1
fi

# Compose build args
CLEAN_ARGS=()
if [[ "$CLEAN" == true ]]; then
    CLEAN_ARGS+=("clean")
fi

BUILD_ARGS=()
if [[ "$SKIP_TESTS" == true ]]; then
    BUILD_ARGS+=("package" "-DskipTests")
else
    BUILD_ARGS+=("package")
fi

if [[ "$PRE_PROFILE" == true ]]; then
    BUILD_ARGS+=("-Ppre")
fi
if [[ "$RELEASE_PROFILE" == true ]]; then
    BUILD_ARGS+=("-Prelease")
fi
if [[ -n "$SPARK_VER" ]]; then
    BUILD_ARGS+=("-Pspark-$SPARK_VER")
fi
if [[ -n "$SCALA_VER" ]]; then
    BUILD_ARGS+=("-Pscala-$SCALA_VER")
fi
if [[ -n "$CELEBORN_VER" ]]; then
    BUILD_ARGS+=("-Pceleborn,celeborn-$CELEBORN_VER")
fi
if [[ -n "$UNIFFLE_VER" ]]; then
    BUILD_ARGS+=("-Puniffle,uniffle-$UNIFFLE_VER")
fi
if [[ -n "$PAIMON_VER" ]]; then
    BUILD_ARGS+=("-Ppaimon,paimon-$PAIMON_VER")
fi
if [[ -n "$FLINK_VER" ]]; then
    BUILD_ARGS+=("-Pflink-$FLINK_VER")
fi

MVN_ARGS=("${CLEAN_ARGS[@]}" "${BUILD_ARGS[@]}")

# -----------------------------------------------------------------------------
# Write build information to auron-build-info.properties
# -----------------------------------------------------------------------------
BUILD_INFO_FILE="common/src/main/resources/auron-build-info.properties"
mkdir -p "$(dirname "$BUILD_INFO_FILE")"

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | awk '{print $3}' | tr -d '"')
PROJECT_VERSION=$(xmllint --xpath "/*[local-name()='project']/*[local-name()='properties']/*[local-name()='project.version']/text()" pom.xml)
RUST_VERSION=$(command -v rustc >/dev/null 2>&1 && rustc --version | awk '{print $2}' || echo "")

{
  echo "spark.version=${SPARK_VER}"
  echo "rust.version=${RUST_VERSION}"
  echo "java.version=${JAVA_VERSION}"
  echo "project.version=${PROJECT_VERSION}"
  echo "scala.version=${SCALA_VER}"
  echo "celeborn.version=${CELEBORN_VER}"
  echo "uniffle.version=${UNIFFLE_VER}"
  echo "paimon.version=${PAIMON_VER}"
  echo "flink.version=${FLINK_VER}"
  echo "build.timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
} > "$BUILD_INFO_FILE"

echo "[INFO] Build info written to $BUILD_INFO_FILE"

# Execute Maven command
if [[ "$USE_DOCKER" == true ]]; then
    echo "[INFO] Compiling inside Docker container using image: $IMAGE_NAME"
    # In Docker mode, use multi-threaded Maven build with -T8 for faster compilation
    BUILD_ARGS+=("-T8")
    if [[ "$CLEAN" == true ]]; then
        # Clean the host-side directory that is mounted into the Docker container.
        # This avoids "device or resource busy" errors when running `mvn clean` inside the container.
        echo "[INFO] Docker mode: manually cleaning target-docker contents..."
        rm -rf ./target-docker/* || echo "[WARN] Failed to clean target-docker/*"
    fi

    echo "[INFO] Compiling inside Docker container..."
    export AURON_BUILD_ARGS="${BUILD_ARGS[*]}"
    export BUILD_CONTEXT="./${IMAGE_NAME}"
    exec docker-compose -f dev/docker-build/docker-compose.yml up --abort-on-container-exit
else
    echo "[INFO] Compiling locally with maven args: $MVN_CMD ${MVN_ARGS[@]} $@"
    "$MVN_CMD" "${MVN_ARGS[@]}" "$@"
fi
