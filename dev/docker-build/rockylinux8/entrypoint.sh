#!/bin/bash

# Switch JDK based on AURON_JAVA_VERSION env var (default: 8)
case "${AURON_JAVA_VERSION:-8}" in
  17) export JAVA_HOME="/usr/lib/jvm/java-17" ;;
  21) export JAVA_HOME="/usr/lib/jvm/java-21" ;;
  *)  export JAVA_HOME="/usr/lib/jvm/java-8" ;;
esac
export PATH="$JAVA_HOME/bin:$PATH"

exec "$@"
