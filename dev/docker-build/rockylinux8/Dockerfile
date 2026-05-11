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

# ---------------------------------------------------------------------
# Apache Auron build environment on Rocky Linux 8
# ---------------------------------------------------------------------
FROM rockylinux:8

LABEL maintainer="Apache Auron Team"
LABEL description="Rocky Linux 8 build environment for Auron project"

# ---------------------------------------------------------------------
# Basic environment setup
# ---------------------------------------------------------------------
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive

# ---------------------------------------------------------------------
# Update system and install base packages (no EPEL)
# ---------------------------------------------------------------------
RUN dnf -y update && \
    dnf -y groupinstall "Development Tools" && \
    dnf -y install \
        git curl wget unzip zip \
        openssl openssl-devel \
        zlib-devel pkgconfig \
        ca-certificates dnf-plugins-core && \
    # Disable EPEL repos if any exist (avoid metadata errors)
    if ls /etc/yum.repos.d/epel*.repo >/dev/null 2>&1; then \
        sed -i 's/^enabled=1/enabled=0/' /etc/yum.repos.d/epel*.repo; \
    fi && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# ---------------------------------------------------------------------
# Verify GCC / G++
# ---------------------------------------------------------------------
RUN gcc --version && g++ --version

# ---------------------------------------------------------------------
# Install Rust nightly toolchain
# ---------------------------------------------------------------------
RUN curl https://sh.rustup.rs -sSf -o rustup-init && \
    chmod +x rustup-init && \
    ./rustup-init -y --default-toolchain nightly-2025-05-09-x86_64-unknown-linux-gnu && \
    rm rustup-init
ENV PATH="/root/.cargo/bin:${PATH}"

# ---------------------------------------------------------------------
# Install Java (OpenJDK 8 and OpenJDK 21)
# ---------------------------------------------------------------------
RUN dnf -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel \
                   java-21-openjdk java-21-openjdk-devel && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Default to JDK 8; override at runtime via JAVA_VERSION env var
ENV JAVA_8_HOME="/usr/lib/jvm/java-1.8.0-openjdk"
ENV JAVA_21_HOME="/usr/lib/jvm/java-21-openjdk"
ENV JAVA_HOME="${JAVA_8_HOME}"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN echo 'export JAVA_8_HOME="/usr/lib/jvm/java-1.8.0-openjdk"' >> /root/.bashrc && \
    echo 'export JAVA_21_HOME="/usr/lib/jvm/java-21-openjdk"' >> /root/.bashrc && \
    echo 'if [ "$JAVA_VERSION" = "21" ]; then export JAVA_HOME="$JAVA_21_HOME"; else export JAVA_HOME="$JAVA_8_HOME"; fi' >> /root/.bashrc && \
    echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> /root/.bashrc

# ---------------------------------------------------------------------
# Set working directory
# ---------------------------------------------------------------------
WORKDIR /auron

# ---------------------------------------------------------------------
# Default command
# ---------------------------------------------------------------------
CMD ["/bin/bash"]