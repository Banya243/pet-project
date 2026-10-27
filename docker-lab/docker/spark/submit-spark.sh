#!/usr/bin/env bash
set -euo pipefail

export SPARK_HOME=/opt/spark
export JAVA_HOME=/opt/java/openjdk
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"

# SSHOperator and docker compose exec both submit from this container.
exec /opt/spark/bin/spark-submit "$@"
