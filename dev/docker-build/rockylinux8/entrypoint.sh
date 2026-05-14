#!/bin/bash

source ~/.bashrc 2>/dev/null || true

# Determine JDK version:
# - If AURON_JAVA_VERSION is set, use it directly
# - Otherwise, auto-detect from AURON_BUILD_ARGS (Spark 4.x requires JDK 17+)
if [ -z "$AURON_JAVA_VERSION" ]; then
  if echo "$AURON_BUILD_ARGS" | grep -qE '\-Pspark-4\.'; then
    AURON_JAVA_VERSION="21"
  else
    AURON_JAVA_VERSION="8"
  fi
fi

case "$AURON_JAVA_VERSION" in
  17) export JAVA_HOME="/usr/lib/jvm/java-17" ;;
  21) export JAVA_HOME="/usr/lib/jvm/java-21" ;;
  *)  export JAVA_HOME="/usr/lib/jvm/java-8" ;;
esac
export PATH="$JAVA_HOME/bin:$PATH"

exec "$@"
