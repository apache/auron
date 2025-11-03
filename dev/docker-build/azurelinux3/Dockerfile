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
# Apache Auron build environment on Azure Linux 3.0
# ---------------------------------------------------------------------
FROM mcr.microsoft.com/azurelinux/base/core:3.0

LABEL maintainer="Apache Auron Team"
LABEL description="Azure Linux 3.0 build environment for Auron project"

# ---------------------------------------------------------------------
# Basic environment setup
# ---------------------------------------------------------------------
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV TZ=UTC

# ---------------------------------------------------------------------
# Update system and install base packages
# ---------------------------------------------------------------------
RUN tdnf -y update && \
    tdnf -y install \
        build-essential \
        git curl wget unzip zip \
        openssl openssl-devel \
        zlib-devel pkg-config \
        ca-certificates \
        gcc gcc-c++ \
        make \
        tar gzip \
        which && \
    tdnf clean all && \
    rm -rf /var/cache/tdnf

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
# Install Java (Microsoft OpenJDK 17 - matches Azure Synapse Spark pools)
# ---------------------------------------------------------------------
RUN tdnf -y install msopenjdk-17 && \
    tdnf clean all && \
    rm -rf /var/cache/tdnf

# Set JAVA_HOME for Microsoft OpenJDK 17 on Azure Linux
ENV JAVA_HOME="/usr/lib/jvm/msopenjdk-17"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ---------------------------------------------------------------------
# Create .bashrc with necessary environment variables
# ---------------------------------------------------------------------
RUN echo 'export PATH="/root/.cargo/bin:${PATH}"' > /root/.bashrc && \
    echo 'export JAVA_HOME="/usr/lib/jvm/msopenjdk-17"' >> /root/.bashrc && \
    echo 'export PATH="${JAVA_HOME}/bin:${PATH}"' >> /root/.bashrc

# ---------------------------------------------------------------------
# Set working directory
# ---------------------------------------------------------------------
WORKDIR /auron

# ---------------------------------------------------------------------
# Default command
# ---------------------------------------------------------------------
CMD ["/bin/bash"]
