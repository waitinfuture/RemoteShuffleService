export ESS_CONF_DIR=$PWD/ess-conf

ps aux | grep "com.aliyun.emr.jss.service.deploy.master.Master" | grep -v grep | awk '{print $2}' | xargs kill -9
ps aux | grep "com.aliyun.emr.jss.service.deploy.worker.Worker" | grep -v grep | awk '{print $2}' | xargs kill -9

nohup java -cp .:jss-service/target/jss-service-1.0.0-SNAPSHOT-shaded.jar com.aliyun.emr.jss.service.deploy.master.Master > master.log 2>&1 &

sleep 5

nohup java -cp .:jss-service/target/jss-service-1.0.0-SNAPSHOT-shaded.jar com.aliyun.emr.jss.service.deploy.worker.Worker > worker1.log 2>&1 &
nohup java -cp .:jss-service/target/jss-service-1.0.0-SNAPSHOT-shaded.jar com.aliyun.emr.jss.service.deploy.worker.Worker > worker2.log 2>&1 &
nohup java -cp .:jss-service/target/jss-service-1.0.0-SNAPSHOT-shaded.jar com.aliyun.emr.jss.service.deploy.worker.Worker > worker3.log 2>&1 &




