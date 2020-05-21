package com.aliyun.emr.jss.service.deploy.worker

import com.aliyun.emr.jss.common.internal.Logging
import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEnv}
import com.aliyun.emr.jss.common.EssConf
import com.aliyun.emr.jss.protocol.RpcNameConstants

private[deploy] object TestWorker2 extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new EssConf
    val workerArgs = new WorkerArguments(args, conf)

    val rpcEnv = RpcEnv.create(RpcNameConstants.WORKER_SYS,
      workerArgs.host,
      workerArgs.port,
      conf)

    val masterAddresses = RpcAddress.fromJindoURL(workerArgs.master)
    rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP,
      new Worker(rpcEnv, workerArgs.memory,
        masterAddresses, RpcNameConstants.WORKER_EP, conf))
    rpcEnv.awaitTermination()
  }
}
