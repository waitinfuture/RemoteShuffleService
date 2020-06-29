#!/usr/bin/env bash

export ESS_CONF_DIR=$PWD/conf

ps aux | grep "service.deploy.master.Master" | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | grep "service.deploy.worker.Worker" | grep -v grep | awk '{print $2}' | xargs kill -9

mkdir -p logs

nohup java -cp .:"$ESS_CONF_DIR":service/target/ess-service-1.0.0-SNAPSHOT-shaded.jar com.aliyun.emr.ess.service.deploy.master.Master > logs/master.log 2>&1 &

sleep 5

nohup java -cp .:"$ESS_CONF_DIR":service/target/ess-service-1.0.0-SNAPSHOT-shaded.jar com.aliyun.emr.ess.service.deploy.worker.Worker > logs/worker1.log 2>&1 &
nohup java -cp .:"$ESS_CONF_DIR":service/target/ess-service-1.0.0-SNAPSHOT-shaded.jar com.aliyun.emr.ess.service.deploy.worker.Worker > logs/worker2.log 2>&1 &
nohup java -cp .:"$ESS_CONF_DIR":service/target/ess-service-1.0.0-SNAPSHOT-shaded.jar com.aliyun.emr.ess.service.deploy.worker.Worker > logs/worker3.log 2>&1 &




