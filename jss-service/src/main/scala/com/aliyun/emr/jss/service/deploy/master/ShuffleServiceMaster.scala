package com.aliyun.emr.jss.service.deploy.master

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}
import com.aliyun.emr.jss.common.util.{ThreadUtils, Utils}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

class ShuffleServiceMaster(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    val conf: SparkConf) extends ThreadSafeRpcEndpoint with Logging {

  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")

  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  override def onStart(): Unit = {
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
  }
  override def onStop(): Unit = {}

  override def receive: PartialFunction[Any, Unit] = {
    null
  }

  override def receiveAndReply(context: _root_.com.aliyun.emr.jss.common.rpc.RpcCallContext): _root_.scala.PartialFunction[Any, Unit] = {
    null
  }
}

object ShuffleServiceMaster extends Logging {

  val SYSTEM_NAME = "jssMaster"
  val ENDPOINT_NAME = "Master"

  def main(args: Array[String]): Unit = {

//    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
//      exitOnUncaughtException = false))
//    Utils.initDaemon(log)
    val conf = new SparkConf
    val masterArgs = new ShuffleServiceMasterArguments(args, conf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, masterArgs.host, masterArgs.port, conf)
    rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new ShuffleServiceMaster(rpcEnv, rpcEnv.address, conf))
    rpcEnv.awaitTermination()
  }

}
