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

FROM centos:7

# install common tools
RUN echo "sslverify=false" >> /etc/yum.conf
RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/*.repo
RUN yum update -y
RUN yum install -y centos-release-scl epel-release
RUN sed -i "s/mirror.centos.org/vault.centos.org/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^#.*baseurl=http/baseurl=https/g" /etc/yum.repos.d/*.repo
RUN sed -i "s/^mirrorlist/#mirrorlist/g" /etc/yum.repos.d/*.repo
RUN sed -i "s|^metalink=|#metalink=|g" /etc/yum.repos.d/epel.repo
RUN sed -i "s|https://download.fedoraproject.org/pub/epel|https://archives.fedoraproject.org/pub/archive/epel|g" /etc/yum.repos.d/epel.repo
RUN yum clean all && yum makecache
RUN yum install -y libzip unzip openssl-devel

# install gcc-11
RUN yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++
RUN echo '. /opt/rh/devtoolset-11/enable' >> ~/.bashrc

# install rust nightly toolchain
RUN curl https://sh.rustup.rs > /rustup-init
RUN chmod +x /rustup-init
RUN /rustup-init -y --default-toolchain nightly-2025-05-09-x86_64-unknown-linux-gnu

# install java (multiple Temurin OpenJDK versions)
# NOTE: JDK 8 and JDK 11 are EOL. Prefer JDK 17+ for new usage.
ARG JDK_VERSIONS="8 11 17 21"
ARG DEFAULT_JDK="8"
ENV JAVA_INSTALL_BASE="/opt/java"
RUN set -eux; \
    mkdir -p "${JAVA_INSTALL_BASE}"; \
    for v in ${JDK_VERSIONS}; do \
      url="https://github.com/adoptium/temurin${v}-binaries/releases/latest/download/OpenJDK${v}U-jdk_x64_linux_hotspot.tar.gz"; \
      curl -fsSL "$url" -o /tmp/temurin.tar.gz || { echo "Failed to download JDK ${v}"; exit 1; }; \
      mkdir -p "${JAVA_INSTALL_BASE}/temurin-${v}"; \
      tar -xzf /tmp/temurin.tar.gz -C "${JAVA_INSTALL_BASE}/temurin-${v}" --strip-components=1; \
    done; \
    rm -f /tmp/temurin.tar.gz; \
    alt="$(command -v update-alternatives || command -v alternatives)"; \
    for v in ${JDK_VERSIONS}; do \
      "$alt" --install /usr/bin/java java "${JAVA_INSTALL_BASE}/temurin-${v}/bin/java" $((100 + v)); \
      "$alt" --install /usr/bin/javac javac "${JAVA_INSTALL_BASE}/temurin-${v}/bin/javac" $((100 + v)); \
    done; \
    "$alt" --set java "${JAVA_INSTALL_BASE}/temurin-${DEFAULT_JDK}/bin/java"; \
    "$alt" --set javac "${JAVA_INSTALL_BASE}/temurin-${DEFAULT_JDK}/bin/javac"
# Default JAVA_HOME is set at build time; run `source /etc/profile.d/java.sh` after set-java to update.
ENV JAVA_HOME="${JAVA_INSTALL_BASE}/temurin-${DEFAULT_JDK}"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Helper to switch JDK at runtime (e.g., set-java 11)
RUN printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'ver="${1:-}"' \
    'if [ -z "$ver" ]; then echo "usage: set-java <version>"; exit 2; fi' \
    'base="/opt/java/temurin-${ver}"' \
    'if [ ! -x "${base}/bin/java" ]; then echo "JDK ${ver} not installed"; exit 1; fi' \
    'alt="$(command -v update-alternatives || command -v alternatives)"' \
    '"$alt" --set java "${base}/bin/java"' \
    '"$alt" --set javac "${base}/bin/javac"' \
    'echo "Java switched to version ${ver}. Run '\''source /etc/profile.d/java.sh'\'' to update JAVA_HOME."' \
    > /usr/local/bin/set-java && chmod +x /usr/local/bin/set-java

# Dynamic JAVA_HOME for interactive shells
RUN printf '%s\n' \
    'export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"' \
    'export PATH="${JAVA_HOME}/bin:${PATH}"' \
    > /etc/profile.d/java.sh

# set working directory
WORKDIR /auron
