#!/usr/bin/env bash

# Starts the ess worker on the machine this script is executed on.

if [ -z "${ESS_HOME}" ]; then
  export ESS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${ESS_HOME}/sbin/ess-config.sh"

if [ "$ESS_WORKER_MEMORY" = "" ]; then
  ESS_WORKER_MEMORY="1g"
fi

if [ "$ESS_WORKER_OFFHEAP_MEMORY" = "" ]; then
  ESS_WORKER_OFFHEAP_MEMORY="1g"
fi

export ESS_JAVA_OPTS="-Xmx$ESS_WORKER_MEMORY -XX:MaxDirectMemorySize=$ESS_WORKER_OFFHEAP_MEMORY $ESS_WORKER_JAVA_OPTS"

WORKER_INSTANCE="$@"
if [ "$WORKER_INSTANCE" = "" ]; then
  WORKER_INSTANCE=1
fi

"${ESS_HOME}/sbin/ess-daemon.sh" start com.aliyun.emr.ess.service.deploy.worker.Worker "$WORKER_INSTANCE"