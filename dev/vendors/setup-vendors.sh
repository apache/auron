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

# =============================================================================
# Setup vendored third-party source trees.
#
# This script clones upstream projects at pinned tags/commits into
# dev/vendors/packages/<name> and applies the local patch series stored under
# dev/vendors/patches/<name>/.
#
# Usage:
#   ./dev/vendors/setup-vendors.sh <name> [name ...]
#   ./dev/vendors/setup-vendors.sh all
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PACKAGES_DIR="${SCRIPT_DIR}/packages"
PATCHES_DIR="${SCRIPT_DIR}/patches"

# -----------------------------------------------------------------------------
# Vendor definitions: name -> upstream repo url, tag/commit
#
# To add a new vendor, append "name|repo_url|tag" to VENDOR_DEFS.
# -----------------------------------------------------------------------------
VENDOR_DEFS=(
  "datafusion|https://github.com/apache/datafusion.git|49.0.0"
  "arrow-rs|https://github.com/apache/arrow-rs.git|55.2.0"
  "orc-rust|https://github.com/datafusion-contrib/orc-rust.git|v0.7.1"
  "serde_json|https://github.com/serde-rs/json.git|v1.0.96"
)

vendor_lookup() {
  local key="$1" field="$2"
  local def
  for def in "${VENDOR_DEFS[@]}"; do
    local name repo tag
    name="${def%%|*}"
    rest="${def#*|}"
    repo="${rest%%|*}"
    tag="${rest#*|}"
    if [[ "${name}" == "${key}" ]]; then
      case "${field}" in
        repo) echo "${repo}" ;;
        tag)  echo "${tag}"  ;;
      esac
      return 0
    fi
  done
  return 1
}

# -----------------------------------------------------------------------------
# Apply a series of numbered patch files to the working tree via git apply.
# -----------------------------------------------------------------------------
apply_patches() {
  local name="$1"
  local patch_dir="${PATCHES_DIR}/${name}"
  local target_dir="${PACKAGES_DIR}/${name}"

  if [[ ! -d "${patch_dir}" ]]; then
    echo "[WARN] No patch directory found for ${name} at ${patch_dir}, skipping patches."
    return 0
  fi

  local patches=()
  local f
  while IFS= read -r f; do patches+=("$f"); done < <(ls -1 "${patch_dir}"/*.patch 2>/dev/null | sort)

  if [[ ${#patches[@]} -eq 0 ]]; then
    echo "[INFO] No patches to apply for ${name}."
    return 0
  fi

  echo "[INFO] Applying ${#patches[@]} patch(es) for ${name} ..."
  local i=0
  for patch in "${patches[@]}"; do
    i=$((i + 1))
    echo "  [${i}/${#patches[@]}] $(basename "${patch}")"
    (cd "${target_dir}" && git apply --3way "${patch}")
  done
}

# -----------------------------------------------------------------------------
# Setup a single vendored project.
# -----------------------------------------------------------------------------
setup_vendor() {
  local name="$1"
  local repo tag target_dir
  repo="$(vendor_lookup "${name}" repo)" || true
  tag="$(vendor_lookup "${name}" tag)" || true
  target_dir="${PACKAGES_DIR}/${name}"

  if [[ -z "${repo:-}" ]]; then
    echo "[ERROR] Unknown vendor: ${name}" >&2
    exit 1
  fi

  if [[ -f "${target_dir}/Cargo.toml" ]]; then
    echo "[INFO] ${name} already set up at ${target_dir}, skipping."
    return 0
  fi

  echo "[INFO] Setting up vendor: ${name}"
  echo "[INFO]   repo: ${repo}"
  echo "[INFO]   tag:  ${tag}"
  echo "[INFO]   dest: ${target_dir}"

  if [[ -d "${target_dir}" ]]; then
    echo "[INFO] Target directory already exists, removing: ${target_dir}"
    rm -rf "${target_dir}"
  fi

  mkdir -p "${PACKAGES_DIR}"

  echo "[INFO] Cloning ${repo} (shallow, tag ${tag}) ..."
  git clone --quiet --depth 1 --branch "${tag}" "${repo}" "${target_dir}"

  apply_patches "${name}"

  echo "[INFO] ${name} setup complete."
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
main() {
  local names=("$@")
  if [[ $# -eq 0 ]] || [[ "$1" == "all" ]]; then
    names=()
    local def
    for def in "${VENDOR_DEFS[@]}"; do
      names+=("${def%%|*}")
    done
  fi

  for name in "${names[@]}"; do
    setup_vendor "${name}"
  done

  echo "[INFO] All vendors setup complete."
}

main ${1+"$@"}
