#!/usr/bin/env bash

# shellcheck disable=SC2068
echo ${@}

usage="Usage: restart_cluster.sh master_host master_port worker_hosts local_disks_volume image_tag worker_cpu_limit worker_memory_limit [old_master_host]"
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
local_disks_volume=$1
shift
image_tag=$1
shift
worker_cpu_limit=$1
shift
worker_memory_limit=$1
shift
old_master_host=$1

echo "Worker Cpu limit:${worker_cpu_limit}"
echo "Worker Mem limit:${worker_memory_limit}"

echo "Image tag ${image_tag}"
echo "Local disk volume ${local_disks_volume}"

echo "Make user directory"
echo "sudo salt -L ${worker_hosts} cmd.run \"export USER=$(echo $USER) && mkdir -p /home/$(echo $USER)\""
sudo salt -L ${worker_hosts} cmd.run "export USER=$(echo $USER) && mkdir -p /home/$(echo $USER)"

echo "Copy ess images"
# https://github.com/saltstack/salt/issues/16592
# 低于2017的salt版本只能使用cp.get_dir，否则可以使用chunk模式 https://github.com/saltstack/salt/pull/41216
#sudo salt -L ${worker_hosts} cp.get_dir salt://ess-1.0.0-bin-release /home/$(echo $USER)/ess-1.0.0-bni-release
echo "sudo salt -L ${worker_hosts} cp.get_file salt://emr-shuffle-service-image.tgz /home/$(echo $USER)/emr-shuffle-service-image.tgz"
sudo salt -L ${worker_hosts} cp.get_file salt://emr-shuffle-service-image.tgz /home/$(echo $USER)/emr-shuffle-service-image.tgz

echo "Copy ess conf"
sudo salt -L ${worker_hosts} cp.get_dir salt://ess-conf /home/$(echo $USER)/

# echo "Copy ess conf"
# sudo salt -L ${worker_hosts} cp.get_dir salt://conf /home/$(echo $USER)/

echo "Stop Master" + ${master_host}
echo "sudo salt ${master_host} cmd.run \"sudo docker stop ess_master\""
sudo salt ${master_host} cmd.run "sudo docker stop ess_master" runas=$(echo $USER)
echo "sudo salt ${master_host} cmd.run \"sudo docker rm -f ess_master\""
sudo salt ${master_host} cmd.run "sudo docker rm -f ess_master" runas=$(echo $USER)

echo "Stop old Master" + ${old_master_host}
echo "sudo salt ${old_master_host} cmd.run \"sudo docker stop ess_master\""
sudo salt ${old_master_host} cmd.run "sudo docker stop ess_master" runas=$(echo $USER)
echo "sudo salt ${old_master_host} cmd.run \"sudo docker rm -f ess_master\""
sudo salt ${old_master_host} cmd.run "sudo docker rm -f ess_master" runas=$(echo $USER)

echo "Stop Workers"
echo "sudo salt -L ${worker_hosts} cmd.run \"sudo docker stop ess_worker\""
sudo salt -L ${worker_hosts} cmd.run "sudo docker stop ess_worker" runas=$(echo $USER)
echo "sudo salt -L ${worker_hosts} cmd.run \"sudo docker rm -f ess_worker\""
sudo salt -L ${worker_hosts} cmd.run "sudo docker rm -f ess_worker" runas=$(echo $USER)

echo "load ess images"
sudo salt -L ${worker_hosts} cmd.run "sudo docker load -i /home/$(echo $USER)/emr-shuffle-service-image.tgz"

echo "Start Master"
sudo salt ${master_host} cmd.run "docker run -d --env ESS_NO_DAEMONIZE=yes --name ess_master --net=host ${image_tag} /usr/bin/tini -s -- /opt/ess-1.0.0-bin-release/sbin/start-master.sh  -p ${master_port}" runas=$(echo $USER)

sleep 3

echo "Start Workers"
sudo salt -L ${worker_hosts} cmd.run "docker run --cpus='${worker_cpu_limit}' --memory='${worker_memory_limit}' -d ${local_disks_volume} -v /home/$(echo $USER)/ess-conf:/opt/ess-1.0.0-bin-release/conf/ --env ESS_NO_DAEMONIZE=yes --name ess_worker --net=host ${image_tag} /usr/bin/tini -s -- /opt/ess-1.0.0-bin-release/sbin/start-worker.sh ess://${master_host}:${master_port}" runas=$(echo $USER)