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
# Apache Auron build environment on Debian 11
# ---------------------------------------------------------------------
FROM debian:11

LABEL maintainer="Apache Auron Team"
LABEL description="Debian 11 build environment for Auron project"

# ---------------------------------------------------------------------
# Environment setup
# ---------------------------------------------------------------------
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive

# ---------------------------------------------------------------------
# Update system and install basic packages
# ---------------------------------------------------------------------
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
      build-essential \
      software-properties-common \
      curl wget git unzip zip \
      pkg-config \
      ca-certificates openssl libssl-dev zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------
# Verify GCC / G++
# ---------------------------------------------------------------------
RUN gcc --version && g++ --version

# ---------------------------------------------------------------------
# Install Rust (nightly toolchain)
# ---------------------------------------------------------------------
RUN curl https://sh.rustup.rs -sSf -o rustup-init && \
    chmod +x rustup-init && \
    ./rustup-init -y --default-toolchain nightly-2025-05-09-x86_64-unknown-linux-gnu && \
    rm rustup-init
ENV PATH="/root/.cargo/bin:${PATH}"

# ---------------------------------------------------------------------
# Install Java (OpenJDK 11)
# ---------------------------------------------------------------------
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ---------------------------------------------------------------------
# Set working directory
# ---------------------------------------------------------------------
WORKDIR /auron

# ---------------------------------------------------------------------
# Default command
# ---------------------------------------------------------------------
CMD ["/bin/bash"]