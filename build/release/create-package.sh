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

set -o pipefail
set -e
set -x

SKIP_GPG=${SKIP_GPG:-false}

exit_with_usage() {
  local NAME=$(basename $0)
  cat << EOF
Usage: $NAME <source>

Top level targets are:
  source: Create source release tarball

All other inputs are environment variables:

SKIP_GPG        - (optional) Default false
EOF
  exit 1
}

PROJECT_DIR="$(cd "$(dirname "$0")"/../..; pwd)"
RELEASE_DIR="${PROJECT_DIR}/tmp"

RELEASE_VERSION=$(grep '<project.version>.*</project.version>' "${PROJECT_DIR}/pom.xml" -o \
                | head -n 1 \
                | sed 's/<\/*project.version>//g')

SHASUM="sha512sum"
if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
fi

package_source() {
  SRC_TGZ_FILE="apache-auron-${RELEASE_VERSION}-source.tgz"
  SRC_TGZ="${RELEASE_DIR}/${SRC_TGZ_FILE}"

  mkdir -p "${RELEASE_DIR}"
  rm -f "${SRC_TGZ}*"

  echo "Creating source release tarball ${SRC_TGZ_FILE}"

  git archive --prefix="apache-auron-${RELEASE_VERSION}-source/" -o "${SRC_TGZ}" HEAD

  if [ "$SKIP_GPG" == "false" ] ; then
    gpg --armor --detach-sig "${SRC_TGZ}"
  fi
  (cd "${RELEASE_DIR}" && $SHASUM "${SRC_TGZ_FILE}" > "${SRC_TGZ_FILE}.sha512")
}

if [[ "$1" == "source" ]]; then
  package_source
  exit 0
fi

if [[ "$1" == "binary" ]]; then
  package_binary
  exit 0
fi

exit_with_usage
