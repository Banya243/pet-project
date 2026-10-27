#!/usr/bin/env bash
set -euo pipefail

case "${1:-gateway}" in
  master)
    exec gosu spark /opt/spark/bin/spark-class \
      org.apache.spark.deploy.master.Master \
      --host spark-master --port 7077 --webui-port 8080
    ;;
  worker)
    exec gosu spark /opt/spark/bin/spark-class \
      org.apache.spark.deploy.worker.Worker \
      --cores 2 --memory 1536m --webui-port 8081 spark://spark-master:7077
    ;;
  gateway)
    ssh-keygen -A
    exec /usr/sbin/sshd -D -e
    ;;
  *)
    exec "$@"
    ;;
esac
