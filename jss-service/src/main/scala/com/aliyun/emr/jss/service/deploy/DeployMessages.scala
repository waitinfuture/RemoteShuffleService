package com.aliyun.emr.jss.service.deploy

import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEndpointRef}
import com.aliyun.emr.jss.common.util.Utils

private[deploy] sealed trait DeployMessage extends Serializable

private[deploy] object DeployMessages {

  // Worker to Master
  case class RegisterWorker(
      id: String,
      host: String,
      port: Int,
      memory: Int,
      worker: RpcEndpointRef) extends DeployMessage {
    Utils.checkHost(host)
    assert (port > 0)
  }

  case class Heartbeat(workerId: String, worker: RpcEndpointRef) extends DeployMessage

  // Master to Worker

  sealed trait RegisterWorkerResponse

  case class RegisteredWorker(master: RpcEndpointRef, masterAddress: RpcAddress) extends DeployMessage with RegisterWorkerResponse

  case class RegisterWorkerFailed(message: String) extends DeployMessage with RegisterWorkerResponse

  case object ReregisterWithMaster // used when a worker attempts to reconnect to a master

  case object SendHeartbeat

}
