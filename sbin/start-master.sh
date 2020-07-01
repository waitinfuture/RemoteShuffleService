#!/usr/bin/env bash

# Starts the ess master on the machine this script is executed on.

if [ -z "${ESS_HOME}" ]; then
  export ESS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${ESS_HOME}/sbin/ess-config.sh"

if [ "$ESS_MASTER_MEMORY" = "" ]; then
  ESS_MASTER_MEMORY="1g"
fi

export ESS_JAVA_OPTS="-Xmx$ESS_MASTER_MEMORY $ESS_MASTER_JAVA_OPTS"

"${ESS_HOME}/sbin/ess-daemon.sh" start com.aliyun.emr.ess.service.deploy.master.Master 1