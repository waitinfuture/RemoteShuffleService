#!/usr/bin/env bash

# shellcheck disable=SC2068
echo ${@}

usage="Usage: restart_cluster.sh master_host master_port worker_hosts [old_master_host]"
# if no args specified, show usage
if [ $# -le 2 ]; then
  echo $usage
  exit 1
fi

master_host=$1
shift
master_port=$1
shift
worker_hosts=$1
shift
old_master_host=$1

#echo "Check package"
#if [ ! -f ess-1.0.0-release.tgz ]; then
#    echo "there should exist ess-1.0.0-release.tgz package at path " + $(dirname $(readlink -f $0))
#    exit 1
#fi

echo "Make user directory"
echo "sudo salt -L ${worker_hosts} cmd.run \"export USER=$(echo $USER) && mkdir /home/$(echo $USER)\""
sudo salt -L ${worker_hosts} cmd.run "export USER=$(echo $USER) && mkdir /home/$(echo $USER)"

echo "Copy ess package"
# https://github.com/saltstack/salt/issues/16592
# 低于2017的salt版本只能使用cp.get_dir，否则可以使用chunk模式 https://github.com/saltstack/salt/pull/41216
#sudo salt -L ${worker_hosts} cp.get_dir salt://ess-1.0.0-bin-release /home/$(echo $USER)/ess-1.0.0-bni-release
echo "sudo salt -L ${worker_hosts} cp.get_file salt://ess-1.0.0-release.tgz /home/$(echo $USER)/ess-1.0.0-release.tgz"
sudo salt -L ${worker_hosts} cp.get_file salt://ess-1.0.0-release.tgz /home/$(echo $USER)/ess-1.0.0-release.tgz

echo "Make ess work directory"
make_work_dir="export USER=$(echo $USER) && cd /home/$(echo $USER) && su - $USER -c 'tar -zxvf ess-1.0.0-release.tgz' && cd ess-1.0.0-bin-release"
echo "sudo salt ${old_master_host} cmd.run \"${make_work_dir}\""
sudo salt -L ${worker_hosts} cmd.run "${make_work_dir}"


stepinto_workdir="cd /home/$(echo $USER)/ess-1.0.0-bin-release"
echo "Copy ess conf"
sudo salt -L ${worker_hosts} cp.get_dir salt://conf /home/$(echo $USER)/ess-1.0.0-bin-release/

echo "Stop old Master" + ${old_master_host}
echo "sudo salt ${old_master_host} cmd.run \"${stepinto_workdir} && export USER=$(echo $USER) && sh sbin/stop-master.sh\""
sudo salt ${old_master_host} cmd.run "${stepinto_workdir} && export USER=$(echo $USER)  && sh sbin/stop-master.sh" runas=$(echo $USER)

echo "Stop current Master if alive" + ${master_host}
echo "sudo salt ${master_host} cmd.run \"${stepinto_workdir} && export USER=$(echo $USER)  && sh sbin/stop-master.sh\""
sudo salt ${master_host} cmd.run "${stepinto_workdir} && export USER=$(echo $USER)  && sh sbin/stop-master.sh" runas=$(echo $USER)

echo "Stop Workers"
echo "sudo salt -L ${worker_hosts} cmd.run \"${stepinto_workdir} && export USER=$(echo $USER)  && sh sbin/stop-worker.sh\""
sudo salt -L ${worker_hosts} cmd.run "${stepinto_workdir} && export USER=$(echo $USER)  && sh sbin/stop-worker.sh" runas=$(echo $USER)

echo "Start Master"
echo "sudo salt ${master_host} cmd.run \"${stepinto_workdir} && export USER=$(echo $USER)  && sh sbin/start-master.sh -p ${master_port}\""
sudo salt ${master_host} cmd.run "${stepinto_workdir} && export USER=$(echo $USER)  && sh sbin/start-master.sh -p ${master_port}" runas=$(echo $USER)

sleep 3

echo "Start Workers"
echo "sudo salt -L ${worker_hosts} cmd.run \"${stepinto_workdir} && export USER=$(echo $USER)  && sh sbin/start-worker.sh ess://${master_host}:${master_port}\""
sudo salt -L ${worker_hosts} cmd.run "${stepinto_workdir} && export USER=$(echo $USER)  && sh sbin/start-worker.sh ess://${master_host}:${master_port}" runas=$(echo $USER)
