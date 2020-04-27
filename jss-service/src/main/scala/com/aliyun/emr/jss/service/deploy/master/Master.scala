package com.aliyun.emr.jss.service.deploy.master

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import com.aliyun.emr.jss.common.JindoConf
import com.aliyun.emr.jss.common.internal.Logging
import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}
import com.aliyun.emr.jss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.jss.protocol.RpcNameConstants
import com.aliyun.emr.jss.service.deploy.DeployMessages.{Heartbeat, RegisterWorker, RegisterWorkerFailed, RegisteredWorker}

import scala.collection.mutable

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    val conf: JindoConf) extends ThreadSafeRpcEndpoint with Logging {

  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")

  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  val workers = new mutable.HashSet[WorkerInfo]
  private val idToWorker = new mutable.HashMap[String, WorkerInfo]
  private val addressToWorker = new mutable.HashMap[RpcAddress, WorkerInfo]

  override def onStart(): Unit = {
//    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
//      override def run(): Unit = Utils.tryLogNonFatalError {
//        self.send(CheckForWorkerTimeOut)
//      }
//    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
  }
  override def onStop(): Unit = {}

  override def receive: PartialFunction[Any, Unit] = {
    case RegisterWorker(id, workerHost, workerPort, memory, workerRef) =>
      logInfo("Registering worker %s:%d with %s RAM".format(
        workerHost, workerPort, Utils.megabytesToString(memory)))
      if (idToWorker.contains(id)) {
        workerRef.send(RegisterWorkerFailed("Duplicate worker ID"))
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, memory, workerRef)
        if (registerWorker(worker)) {
          workerRef.send(RegisteredWorker(self, address))
        } else {
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          workerRef.send(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }

    case Heartbeat(workerId, worker) =>
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          if (workers.map(_.id).contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
//            worker.send(ReconnectWorker(masterUrl))
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }

  }

  override def receiveAndReply(context: _root_.com.aliyun.emr.jss.common.rpc.RpcCallContext): _root_.scala.PartialFunction[Any, Unit] = {
    null
  }

  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker, "Worker replaced by a new worker with same address")
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  private def removeWorker(worker: WorkerInfo, msg: String) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address
  }
}

private[deploy] object Master extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new JindoConf()
    val masterArgs = new MasterArguments(args, conf)
    val rpcEnv = RpcEnv.create(
      RpcNameConstants.MASTER_SYS,
      masterArgs.host,
      masterArgs.port,
      conf)
    rpcEnv.setupEndpoint(RpcNameConstants.MASTER_EP,
      new Master(rpcEnv, rpcEnv.address, conf))
    rpcEnv.awaitTermination()
  }
}
