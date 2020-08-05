#!/usr/bin/env bash

# shellcheck disable=SC2068
echo ${@}

usage="Usage: restart_worker.sh master_address"
# if no args specified, show usage
if [ $# -ne 1 ]; then
  echo $usage
  exit 1
fi

master_address=$1

cd /home/$(echo $USER)/ess-1.0.0-release && export USER=$(echo $USER)
echo "stop if check status error"
sh sbin/stop-worker.sh

sleep 5
echo "start worker"
sh sbin/start-worker.sh ${master_address}