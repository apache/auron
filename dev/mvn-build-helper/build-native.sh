#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -eo pipefail

# Preserve the calling directory
_CALLING_DIR="$(pwd)"

PROJECT_DIR="$(cd "`dirname "$0"`/../.."; pwd)"
cd "$PROJECT_DIR"

profile="${1:-release}"
features_arg=""
if [ -n "$2" ]; then
    features_arg="--features $2"
fi

echo "Building with profile: $profile"

# Support cross-compilation via CARGO_BUILD_TARGET environment variable
# Example: CARGO_BUILD_TARGET=x86_64-apple-darwin mvn package
target_arg=""
target_subdir=""
if [ -n "$CARGO_BUILD_TARGET" ]; then
    echo "Cross-compiling for target: $CARGO_BUILD_TARGET"
    target_arg="--target $CARGO_BUILD_TARGET"
    target_subdir="$CARGO_BUILD_TARGET/"
fi

libname=libauron
if [ "$(uname)" == "Darwin" ]; then
    libsuffix=dylib
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    libsuffix=so
elif [ "$(expr substr $(uname -s) 1 10)" == "MINGW64_NT" ]; then
    libname=auron
    libsuffix=dll
else
    echo "Unsupported platform $(uname)"
    exit 1
fi

cache_dir="native-engine/_build/$profile"
cache_libpath="$cache_dir/$libname.$libsuffix"
# Include target in checksum file name to avoid conflicts between architectures
target_suffix="${CARGO_BUILD_TARGET:+-$CARGO_BUILD_TARGET}"
cache_checksum_file="./.build-checksum.$profile.$libname.$libsuffix$target_suffix.cache"
cargo_libpath="target/${target_subdir}$profile/$libname.$libsuffix"

checksum() {
    # Determine whether to use md5sum or md5
    if command -v md5sum >/dev/null 2>&1; then
        hash_cmd="md5sum"
    elif command -v md5 >/dev/null 2>&1; then
        hash_cmd="md5 -r" # Use md5 -r for macOS to match md5sum format
    else
        echo "Neither md5sum nor md5 is available."
        exit 1
    fi

    feat_sum="$(echo "$features_arg" | $hash_cmd | awk '{print $1}' )"

    files_sum="$(
        find Cargo.toml Cargo.lock native-engine "$cache_libpath" | \
        xargs $hash_cmd 2>&1 | \
        sort -k1 | \
        $hash_cmd | awk '{print $1}'
    )"

    printf "%s%s" "$feat_sum" "$files_sum" | $hash_cmd | awk '{print $1}'
}

if [ -f "$cache_libpath" ]; then
  if [ -f "$cache_checksum_file" ]; then
    old_checksum="$(cat "$cache_checksum_file")"
  else
    old_checksum="No checksum file found."
  fi

  new_checksum="$(checksum)"

  echo -e "old build-checksum: \n$old_checksum\n========"
  echo -e "new build-checksum: \n$new_checksum\n========"
fi

if [ ! -f "$cache_libpath" ] || [ "$new_checksum" != "$old_checksum" ]; then
    # Set RUSTFLAGS only if not cross-compiling
    if [ -z "$CARGO_BUILD_TARGET" ]; then
        export RUSTFLAGS=${RUSTFLAGS:-"-C target-cpu=native"}
    else
        echo "Cross-compiling: skipping target-cpu=native flag"
    fi

    echo "Running cargo fix..."
    cargo fix --all --allow-dirty --allow-staged --allow-no-vcs  2>&1

    echo "Running cargo fmt..."
    cargo fmt --all -q -- 2>&1

    echo "Building native with [$profile] profile${target_arg:+ for target $CARGO_BUILD_TARGET}..."
    cargo build --profile="$profile" $target_arg $features_arg --verbose --locked --frozen 2>&1

    mkdir -p "$cache_dir"
    cp -f "$cargo_libpath" "$cache_libpath"

    new_checksum="$(checksum)"
    echo "build-checksum updated: $new_checksum"
    echo "$new_checksum" >"$cache_checksum_file"
else
    echo "native-engine source code and built libraries not modified, no need to rebuild"
fi

echo "Native build completed successfully"

cd "${_CALLING_DIR}"
