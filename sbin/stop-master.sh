#!/usr/bin/env bash

# Stops the ess master on the machine this script is executed on.

if [ -z "${ESS_HOME}" ]; then
  export ESS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

"${ESS_HOME}/sbin/ess-daemon.sh" stop com.aliyun.emr.ess.service.deploy.master.Master 1