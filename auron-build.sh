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
# =============================================================================
# Apache Auron Project Build Script
# Description:
#   This script automates the build process for the Apache Auron project.
# =============================================================================

# -----------------------------------------------------------------------------
# Section: Supported versions constants
# Description:
#   Define constants for supported component versions
# -----------------------------------------------------------------------------
SUPPORTED_OS_IMAGES=("centos7" "ubuntu24" "rockylinux8" "debian11" "azurelinux3")
SUPPORTED_SPARK_VERSIONS=("3.0" "3.1" "3.2" "3.3" "3.4" "3.5")
SUPPORTED_SCALA_VERSIONS=("2.12" "2.13")
SUPPORTED_CELEBORN_VERSIONS=("0.5" "0.6")
# Currently only one supported version, but kept plural for consistency
SUPPORTED_UNIFFLE_VERSIONS=("0.10")
SUPPORTED_PAIMON_VERSIONS=("1.2")
SUPPORTED_FLINK_VERSIONS=("1.18")
SUPPORTED_ICEBERG_VERSIONS=("1.9")

# -----------------------------------------------------------------------------
# Function: print_help
# Description:
#   Print script usage information, supported options, and example commands.
# -----------------------------------------------------------------------------
print_help() {
    echo "Usage: $0 [OPTIONS] <maven build options>"
    echo "Build Auron project with specified Maven profiles"
    echo
    echo "Options:"
    echo "  --pre                    Activate pre-release profile"
    echo "  --release                Activate release profile"
    echo "  --clean <true|false>     Clean before build (default: true)"
    echo "  --skiptests <true|false> Skip unit tests (default: true)"
    echo "  --sparktests <true|false> Run spark tests (default: false)"
    echo "  --docker <true|false>    Build in Docker environment (default: false)"
    IFS=','; echo "  --image <NAME>           Docker image to use (e.g. ${SUPPORTED_OS_IMAGES[*]}, default: ${SUPPORTED_OS_IMAGES[*]:0:1})"; unset IFS
    IFS=','; echo "  --sparkver <VERSION>     Specify Spark version (e.g. ${SUPPORTED_SPARK_VERSIONS[*]})"; unset IFS
    IFS=','; echo "  --flinkver <VERSION>     Specify Flink version (e.g. ${SUPPORTED_FLINK_VERSIONS[*]})"; unset IFS
    IFS=','; echo "  --scalaver <VERSION>     Specify Scala version (e.g. ${SUPPORTED_SCALA_VERSIONS[*]})"; unset IFS
    IFS=','; echo "  --celeborn <VERSION>     Specify Celeborn version (e.g. ${SUPPORTED_CELEBORN_VERSIONS[*]})"; unset IFS
    IFS=','; echo "  --uniffle <VERSION>      Specify Uniffle version (e.g. ${SUPPORTED_UNIFFLE_VERSIONS[*]})"; unset IFS
    IFS=','; echo "  --paimon <VERSION>       Specify Paimon version (e.g. ${SUPPORTED_PAIMON_VERSIONS[*]})"; unset IFS
    IFS=','; echo "  --iceberg <VERSION>      Specify Iceberg version (e.g. ${SUPPORTED_ICEBERG_VERSIONS[*]})"; unset IFS

    echo "  -h, --help               Show this help message"
    echo
    echo "Examples:"
    echo "  $0 --pre --sparkver ${SUPPORTED_SPARK_VERSIONS[*]: -1}" \
         "--scalaver ${SUPPORTED_SCALA_VERSIONS[*]: -1} -DskipBuildNative"
    echo "  $0 --docker true --image ${SUPPORTED_OS_IMAGES[*]:0:1}" \
         "--clean true --skiptests true --release" \
         "--sparkver ${SUPPORTED_SPARK_VERSIONS[*]: -1}" \
         "--flinkver ${SUPPORTED_FLINK_VERSIONS[*]: -1}" \
         "--scalaver ${SUPPORTED_SCALA_VERSIONS[*]: -1}" \
         "--celeborn ${SUPPORTED_CELEBORN_VERSIONS[*]: -1}" \
         "--uniffle ${SUPPORTED_UNIFFLE_VERSIONS[*]: -1}" \
         "--paimon ${SUPPORTED_PAIMON_VERSIONS[*]: -1}" \
         "--iceberg ${SUPPORTED_ICEBERG_VERSIONS[*]: -1}"
    exit 0
}

# -----------------------------------------------------------------------------
# Function: check_supported_version
# Description:
#   Validate if a provided version string exists in the supported version list.
# -----------------------------------------------------------------------------
check_supported_version() {
  local value="$1"
  shift
  local arr=("$@")
  for v in "${arr[@]}"; do
    [[ "$v" == "$value" ]] && return 0
  done
  return 1
}

# -----------------------------------------------------------------------------
# Function: print_invalid_option_error
# Description:
#   Print formatted error message for unsupported version or option.
# -----------------------------------------------------------------------------
print_invalid_option_error() {
    local component="$1"
    local invalid_value="$2"
    shift 2
    local supported_list
    supported_list=$(IFS=','; echo "$*")

    echo "ERROR: Unsupported $component: $invalid_value, currently supported: ${supported_list}" >&2
    exit 1
}

MVN_CMD="$(dirname "$0")/build/mvn"

# -----------------------------------------------------------------------------
# Section: Initialize Variables
# Description:
#   Initialize basic variables and default options for the build
# -----------------------------------------------------------------------------
USE_DOCKER=false
IMAGE_NAME="${SUPPORTED_OS_IMAGES[*]:0:1}"
PRE_PROFILE=false
RELEASE_PROFILE=false
CLEAN=true
SKIP_TESTS=true
SPARK_TESTS=false
SPARK_VER=""
FLINK_VER=""
SCALA_VER=""
CELEBORN_VER=""
UNIFFLE_VER=""
PAIMON_VER=""
ICEBERG_VER=""

# -----------------------------------------------------------------------------
# Section: Argument Parsing
# Description:
#   Parse command-line options and validate their values.
# -----------------------------------------------------------------------------
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
                if ! check_supported_version "$IMAGE_NAME" "${SUPPORTED_OS_IMAGES[@]}"; then
                  print_invalid_option_error Image "$IMAGE_NAME" "${SUPPORTED_OS_IMAGES[@]}"
                fi
                shift 2
            else
                IFS=','; echo "ERROR: Missing argument for --image," \
                "specify one of: ${SUPPORTED_OS_IMAGES[*]}" >&2; unset IFS
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
        --sparktests)
            if [[ -n "$2" && "$2" =~ ^(true|false)$ ]]; then
                SPARK_TESTS="$2"
                shift 2
            else
                echo "ERROR: --sparktests requires true/false" >&2
                exit 1
            fi
            ;;
        --sparkver)
            if [[ -n "$2" && "$2" != -* ]]; then
                SPARK_VER="$2"
                if ! check_supported_version "$SPARK_VER" "${SUPPORTED_SPARK_VERSIONS[@]}"; then
                  print_invalid_option_error Spark "$SPARK_VER" "${SUPPORTED_SPARK_VERSIONS[@]}"
                fi
                shift 2
            else
                IFS=','; echo "ERROR: Missing argument for --sparkver," \
                "specify one of: ${SUPPORTED_SPARK_VERSIONS[*]}" >&2; unset IFS
                exit 1
            fi
            ;;
        --scalaver)
            if [[ -n "$2" && "$2" != -* ]]; then
                SCALA_VER="$2"
                if ! check_supported_version "$SCALA_VER" "${SUPPORTED_SCALA_VERSIONS[@]}"; then
                  print_invalid_option_error Scala "$SCALA_VER" "${SUPPORTED_SCALA_VERSIONS[@]}"
                fi
                shift 2
            else
                IFS=','; echo "ERROR: Missing argument for --scalaver," \
                "specify one of: ${SUPPORTED_SCALA_VERSIONS[*]}" >&2; unset IFS
                exit 1
            fi
            ;;
        --celeborn)
            if [[ -n "$2" && "$2" != -* ]]; then
                CELEBORN_VER="$2"
                if ! check_supported_version "$CELEBORN_VER" "${SUPPORTED_CELEBORN_VERSIONS[@]}"; then
                  print_invalid_option_error Celeborn "$CELEBORN_VER" "${SUPPORTED_CELEBORN_VERSIONS[@]}"
                fi
                shift 2
            else
                IFS=','; echo "ERROR: Missing argument for --celeborn," \
                "specify one of: ${SUPPORTED_CELEBORN_VERSIONS[*]}" >&2; unset IFS
                exit 1
            fi
            ;;
        --uniffle)
            if [[ -n "$2" && "$2" != -* ]]; then
                UNIFFLE_VER="$2"
                if ! check_supported_version "$UNIFFLE_VER" "${SUPPORTED_UNIFFLE_VERSIONS[@]}"; then
                  print_invalid_option_error Uniffle "$UNIFFLE_VER" "${SUPPORTED_UNIFFLE_VERSIONS[@]}"
                fi
                shift 2
            else
                IFS=','; echo "ERROR: Missing argument for --uniffle," \
                "specify one of: ${SUPPORTED_UNIFFLE_VERSIONS[*]}" >&2; unset IFS
                exit 1
            fi
            ;;
        --paimon)
            if [[ -n "$2" && "$2" != -* ]]; then
                PAIMON_VER="$2"
                if ! check_supported_version "$PAIMON_VER" "${SUPPORTED_PAIMON_VERSIONS[@]}"; then
                  print_invalid_option_error Paimon "$PAIMON_VER" "${SUPPORTED_PAIMON_VERSIONS[@]}"
                fi
                shift 2
            else
                IFS=','; echo "ERROR: Missing argument for --paimon," \
                "specify one of: ${SUPPORTED_PAIMON_VERSIONS[*]}" >&2; unset IFS
                exit 1
            fi
            ;;
        --iceberg)
            if [[ -n "$2" && "$2" != -* ]]; then
                ICEBERG_VER="$2"
                if ! check_supported_version "$ICEBERG_VER" "${SUPPORTED_ICEBERG_VERSIONS[@]}"; then
                  print_invalid_option_error Iceberg "$ICEBERG_VER" "${SUPPORTED_ICEBERG_VERSIONS[@]}"
                fi
                if [ -z "$SPARK_VER" ]; then
                  echo "ERROR: Building iceberg requires spark at the same time, and only Spark versions 3.4 or 3.5 are supported."
                  exit 1
                fi
                if [ "$SPARK_VER" != "3.4" ] && [ "$SPARK_VER" != "3.5" ]; then
                  echo "ERROR: Building iceberg requires spark versions are 3.4 or 3.5."
                  exit 1
                fi
                shift 2
            else
                IFS=','; echo "ERROR: Missing argument for --iceberg," \
                "specify one of: ${SUPPORTED_ICEBERG_VERSIONS[*]}" >&2; unset IFS
                exit 1
            fi
            ;;
        --flinkver)
            if [[ -n "$2" && "$2" != -* ]]; then
                FLINK_VER="$2"
                if ! check_supported_version "$FLINK_VER" "${SUPPORTED_FLINK_VERSIONS[@]}"; then
                  print_invalid_option_error Flink "$FLINK_VER" "${SUPPORTED_FLINK_VERSIONS[@]}"
                fi
                shift 2
            else
                IFS=','; echo "ERROR: Missing argument for --flink," \
                "specify one of: ${SUPPORTED_FLINK_VERSIONS[*]}" >&2; unset IFS
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

# -----------------------------------------------------------------------------
# Section: Argument Validation
# Description:
#   Ensure mandatory options are provided and mutually exclusive rules are respected.
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Section: Build Argument Composition
# Description:
#   Assemble Maven command arguments according to user options.
# -----------------------------------------------------------------------------
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

if [[ "$SPARK_TESTS" == true ]]; then
    BUILD_ARGS+=("-Pspark-tests")
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
if [[ -n "$ICEBERG_VER" ]]; then
    BUILD_ARGS+=("-Piceberg-$ICEBERG_VER")
fi

MVN_ARGS=("${CLEAN_ARGS[@]}" "${BUILD_ARGS[@]}")

# -----------------------------------------------------------------------------
# Section: Build Info Generation
# Description:
#   Generate auron-build-info.properties for build metadata tracking.
# -----------------------------------------------------------------------------
BUILD_INFO_FILE="common/src/main/resources/auron-build-info.properties"
mkdir -p "$(dirname "$BUILD_INFO_FILE")"

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | awk '{print $3}' | tr -d '"')
PROJECT_VERSION=$(./build/mvn help:evaluate -N -Dexpression=project.version -Pspark-${SPARK_VER} -q -DforceStdout 2>/dev/null)
RUST_VERSION=$(rustc --version | awk '{print $2}')

get_build_info() {
  case "$1" in
    "spark.version") echo "${SPARK_VER}" ;;
    "rust.version") echo "${RUST_VERSION}" ;;
    "java.version") echo "${JAVA_VERSION}" ;;
    "project.version") echo "${PROJECT_VERSION}" ;;
    "scala.version") echo "${SCALA_VER}" ;;
    "celeborn.version") echo "${CELEBORN_VER}" ;;
    "uniffle.version") echo "${UNIFFLE_VER}" ;;
    "paimon.version") echo "${PAIMON_VER}" ;;
    "flink.version") echo "${FLINK_VER}" ;;
    "iceberg.version") echo "${ICEBERG_VER}" ;;
    "build.timestamp") echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" ;;
    *) echo "" ;;
  esac
}

true > "$BUILD_INFO_FILE"
for key in \
  "spark.version" \
  "rust.version" \
  "java.version" \
  "project.version" \
  "scala.version" \
  "celeborn.version" \
  "uniffle.version" \
  "paimon.version" \
  "flink.version" \
  "iceberg.version" \
  "build.timestamp"; do
  value="$(get_build_info "$key")"
  if [[ -n "$value" ]]; then
    echo "$key=$value" >> "$BUILD_INFO_FILE"
  fi
done

# -----------------------------------------------------------------------------
# Section: Build Info Display
# Description:
#   Output generated build info summary to console.
# -----------------------------------------------------------------------------
echo "[INFO] Starting Apache Auron build..."
if [[ -f "$BUILD_INFO_FILE" ]]; then
    echo "[INFO] Build configuration (from $BUILD_INFO_FILE):"
    while IFS='=' read -r key value; do
        [[ -z "$key" ]] && continue
        echo "[INFO]   $key : $value"
    done < "$BUILD_INFO_FILE"
else
    echo "[WARN] Build info file not found: $BUILD_INFO_FILE"
    echo "[INFO] Use '$0 --help' to view usage instructions and supported versions."
fi

# -----------------------------------------------------------------------------
# Section: Build Execution
# Description:
#   Execute Maven build either locally or inside Docker container.
# -----------------------------------------------------------------------------
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
