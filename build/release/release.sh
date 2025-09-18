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

PROJECT_DIR="$(cd "$(dirname "$0")"/../..; pwd)"

ASF_USERNAME=${ASF_USERNAME:?"ASF_USERNAME is required"}
ASF_PASSWORD=${ASF_PASSWORD:?"ASF_PASSWORD is required"}
RELEASE_RC_NO=${RELEASE_RC_NO:?"RELEASE_RC_NO is required, e.g. 0"}

RELEASE_VERSION=$(grep '<project.version>.*</project.version>' "${PROJECT_DIR}/pom.xml" -o \
                | head -n 1 \
                | sed 's/<\/*project.version>//g')

exit_with_usage() {
  local NAME=$(basename $0)
  cat << EOF
Usage: $NAME <publish|finalize>

Top level targets are:
  publish: Publish tarballs to SVN staging repository
  finalize: Finalize the release after an RC passes vote

All other inputs are environment variables

RELEASE_RC_NO   - Release RC number, (e.g. 0)

ASF_USERNAME - Username of ASF committer account
ASF_PASSWORD - Password of ASF committer account
EOF
  exit 1
}

if [[ ${RELEASE_VERSION} =~ .*-SNAPSHOT ]]; then
  echo "Can not release a SNAPSHOT version: ${RELEASE_VERSION}"
  exit_with_usage
  exit 1
fi

RELEASE_TAG="v${RELEASE_VERSION}-rc${RELEASE_RC_NO}"

SVN_STAGING_REPO="https://dist.apache.org/repos/dist/dev/incubator/auron"
SVN_RELEASE_REPO="https://dist.apache.org/repos/dist/release/incubator/auron"

RELEASE_DIR="${PROJECT_DIR}/tmp"
SVN_STAGING_DIR="${PROJECT_DIR}/tmp/svn-dev"
SVN_RELEASE_DIR="${PROJECT_DIR}/tmp/svn-release"

package() {
  SKIP_GPG="false" $PROJECT_DIR/build/release/create-package.sh source
}

upload_svn_staging() {
  svn checkout --depth=empty "${SVN_STAGING_REPO}" "${SVN_STAGING_DIR}"
  mkdir -p "${SVN_STAGING_DIR}/${RELEASE_TAG}"
  rm -f "${SVN_STAGING_DIR}/${RELEASE_TAG}/*"

  SRC_TGZ_FILE="apache-auron-${RELEASE_VERSION}-source.tgz"

  echo "Copying release tarballs"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}"        "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}.asc"    "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}.asc"
  cp "${RELEASE_DIR}/${SRC_TGZ_FILE}.sha512" "${SVN_STAGING_DIR}/${RELEASE_TAG}/${SRC_TGZ_FILE}.sha512"

  svn add "${SVN_STAGING_DIR}/${RELEASE_TAG}"

  echo "Uploading release tarballs to ${SVN_STAGING_DIR}/${RELEASE_TAG}"
  (
    cd "${SVN_STAGING_DIR}" && \
    svn commit --username "${ASF_USERNAME}" --password "${ASF_PASSWORD}" --message "Apache Auron ${RELEASE_TAG}"
  )
  echo "Auron tarballs uploaded"
}


finalize_svn() {
  echo "Moving Auron tarballs to the release directory"
  svn mv --username "${ASF_USERNAME}" --password "${ASF_PASSWORD}" --no-auth-cache \
     --message "Apache Auron ${RELEASE_VERSION}" \
     "${SVN_STAGING_REPO}/${RELEASE_TAG}" "${SVN_RELEASE_REPO}/auron-${RELEASE_VERSION}"
  echo "Auron tarballs moved"
}

if [[ "$1" == "publish" ]]; then
  package
  upload_svn_staging
  exit 0
fi

if [[ "$1" == "finalize" ]]; then
  echo "THIS STEP IS IRREVERSIBLE! Make sure the vote has passed and you pick the right RC to finalize."
  read -p "You must be a PMC member to run this step. Continue? [y/N] " ANSWER
  if [ "$ANSWER" != "y" ]; then
    echo "Exiting."
    exit 1
  fi

  finalize_svn
  exit 0
fi

exit_with_usage
