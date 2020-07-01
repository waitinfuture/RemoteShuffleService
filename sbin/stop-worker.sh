#!/usr/bin/env bash

# Stops the ess master on the machine this script is executed on.

if [ -z "${ESS_HOME}" ]; then
  export ESS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

WORKER_INSTANCE="$@"
if [ "$WORKER_INSTANCE" = "" ]; then
  WORKER_INSTANCE=1
fi

"${ESS_HOME}/sbin/ess-daemon.sh" stop com.aliyun.emr.ess.service.deploy.worker.Worker "$WORKER_INSTANCE"