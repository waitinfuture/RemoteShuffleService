package com.aliyun.emr.jss.service.deploy.master

import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet, ScheduledFuture, TimeUnit}

import com.aliyun.emr.jss.common.JindoConf
import com.aliyun.emr.jss.common.internal.Logging
import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEndpoint, RpcEnv}
import com.aliyun.emr.jss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ShuffleMessages.{RegisterShuffle, RegisterShufflePartition, RegisterShufflePartitionResponse, RegisterShuffleResponse}
import com.aliyun.emr.jss.service.deploy.DeployMessages._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Random, Success}
import scala.util.control.Breaks._

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    val conf: JindoConf) extends RpcEndpoint with Logging {

  // Threads
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")
  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _
  private val registerShufflePartitionThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "register-shuffle-partition-threadpool",
    8
  )

  // Configs
  private val PARTITION_MEMORY_MB = conf.getSizeAsMb("jindo.partition.memory", "8m").toInt
  private val WORKER_TIMEOUT_MS = conf.getLong("jindo.worker.timeout", 60) * 1000
  private val REAPER_ITERATIONS = 15

  // Structs
  val workers = new ConcurrentSkipListSet[WorkerInfo]()
  private val idToWorker = new ConcurrentHashMap[String, WorkerInfo]()
  private val addressToWorker = new ConcurrentHashMap[RpcAddress, WorkerInfo]
  private val partitionChunkIndex = new ConcurrentHashMap[String, Int]()

  private def getChunkIndex(applicationId: String,
    shuffleId: Int,
    reduceId: Int,
    mode: Int): String = {
    val uniqueId = if (mode == 1) {
      s"${applicationId}_${shuffleId}_${reduceId}_master"
    } else {
      s"${applicationId}_${shuffleId}_${reduceId}_slave"
    }

    val oldIndex = partitionChunkIndex.getOrDefault(uniqueId, -1)
    val newIndex = oldIndex + 1
    partitionChunkIndex.put(uniqueId, newIndex)
    s"${uniqueId}_${newIndex}"
  }

  override def onStart(): Unit = {
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"$address got disassociated, removing it.")
    if (addressToWorker.containsKey(address)) {
      removeWorker(addressToWorker.get(address), s"${address} got disassociated")
    }
  }

  override def receive: PartialFunction[Any, Unit] = {

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

    case Heartbeat(workerId, worker) =>
      idToWorker.get(workerId) match {
        case workerInfo: WorkerInfo =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case null =>
          if (workers.asScala
            .map(w => w.id)
            .contains(workerId)) {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " Asking it to re-register.")
            worker.send(ReconnectWorker)
          } else {
            logWarning(s"Got heartbeat from unregistered worker $workerId." +
              " This worker was never registered, so ignoring the heartbeat.")
          }
      }

    case RegisterWorker(id, workerHost, workerPort, memory, workerRef) =>
      logInfo("Registering worker %s:%d with %s RAM".format(
        workerHost, workerPort, Utils.megabytesToString(memory)))
      if (idToWorker.contains(id)) {
        workerRef.send(RegisterWorkerFailed("Duplicate worker ID"))
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, memory, workerRef)
        if (registerWorker(worker)) {
          workerRef.send(RegisteredWorker)
        } else {
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          workerRef.send(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }
  }

  override def receiveAndReply(context: _root_.com.aliyun.emr.jss.common.rpc.RpcCallContext): _root_.scala.PartialFunction[Any, Unit] = {
    case RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions) =>
      var success: Boolean = true
      val partitionLocation = new ArrayBuffer[PartitionLocation]()
      var successReturnCount: Int = 0
      val futures = new ArrayBuffer[Future[RegisterShufflePartitionResponse]]()
      breakable({
        for (reduceId <- 0 until numPartitions) {
          val picked = choosePartitionLocation(applicationId, shuffleId, reduceId)
          if (picked == null) {
            success = false
            break()
          } else {
            val masterChunkIndex = getChunkIndex(applicationId, shuffleId, reduceId, 1)
            val slaveChunkIndex = getChunkIndex(applicationId, shuffleId, reduceId, 0)

            picked._1.addShufflePartition(masterChunkIndex, PARTITION_MEMORY_MB)
            val m_tmp = picked._1.endpoint.ask[RegisterShufflePartitionResponse](
              RegisterShufflePartition(
                applicationId,
                shuffleId,
                reduceId,
                PARTITION_MEMORY_MB,
                1, masterChunkIndex))
            m_tmp.onComplete({
              case Success(_) =>
                partitionLocation.append(
                  new PartitionLocation(applicationId, shuffleId, reduceId,
                    new URI(picked._1.endpoint.address.toJindoURL))
                )
                successReturnCount += 1
              case Failure(_) =>
                picked._1.removeShufflePartition(masterChunkIndex, PARTITION_MEMORY_MB)
            })(ThreadUtils.sameThread)
            futures.append(m_tmp)

            picked._2.addShufflePartition(slaveChunkIndex, PARTITION_MEMORY_MB)
            val s_tmp = picked._2.endpoint.ask[RegisterShufflePartitionResponse](
              RegisterShufflePartition(
                applicationId,
                shuffleId,
                reduceId,
                PARTITION_MEMORY_MB,
                1, slaveChunkIndex))
            s_tmp.onComplete({
              case Success(_) =>
                successReturnCount += 1
              case Failure(_) =>
                picked._2.removeShufflePartition(slaveChunkIndex, PARTITION_MEMORY_MB)
            })(ThreadUtils.sameThread)
            futures.append(s_tmp)
          }
        }
      })

      for (future <- futures) {
        ThreadUtils.awaitReady(future, Duration.Inf)
      }

      if (successReturnCount != numPartitions * 2) {
        // failed some partition does not register success
        success = false
        // return resource to workerInfo
      }
      context.reply(RegisterShuffleResponse(success, partitionLocation.toList))
  }

  private def choosePartitionLocation(
      applicationId: String,
      shuffleId: Int,
      reduceId: Int): (WorkerInfo, WorkerInfo) = {
    val available = workers.asScala.filter(w => {
      if (w.memoryFree > PARTITION_MEMORY_MB && w.state == WorkerState.ALIVE) {
        true
      } else {
        false
      }
    })

    if (available.size < 2) {
      null
    } else {
      val picked = Random.shuffle(available).take(2).toArray
      (picked.head, picked(1))
    }
  }

  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    workers.asScala.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers.remove(w)
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.containsKey(workerAddress)) {
      val oldWorker = addressToWorker.get(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker, "Worker replaced by a new worker with same address")
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers.add(worker)
    idToWorker.put(worker.id, worker)
    addressToWorker.put(workerAddress, worker)
    true
  }

  private def timeOutDeadWorkers() {
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.asScala
      .filter(w => w.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS)
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning("Removing %s because we got no heartbeat in %d seconds".format(
          worker.id, WORKER_TIMEOUT_MS / 1000))
        removeWorker(worker, s"Not receiving heartbeat for ${WORKER_TIMEOUT_MS / 1000} seconds")
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
          workers.remove(worker)
        }
      }
    }
  }

  private def removeWorker(worker: WorkerInfo, msg: String) {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker.remove(worker.id)
    addressToWorker.remove(worker.endpoint.address)
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
