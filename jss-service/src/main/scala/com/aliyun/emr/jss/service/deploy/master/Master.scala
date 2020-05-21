package com.aliyun.emr.jss.service.deploy.master

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Random

import com.aliyun.emr.jss.common.JindoConf
import com.aliyun.emr.jss.common.internal.Logging
import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEnv}
import com.aliyun.emr.jss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ShuffleMessages._
import com.aliyun.emr.jss.service.deploy.DeployMessages._

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    val conf: JindoConf) extends RpcEndpoint with Logging {

  type WorkerResource = java.util.Map[WorkerInfo,
    (java.util.List[PartitionLocation], java.util.List[PartitionLocation])]

  // Threads
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")
  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _
  private val registerShufflePartitionThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "register-shuffle-partition-threadpool",
    8
  )

  // Configs
  private val PARTITION_MEMORY_MB = conf.getSizeAsMb("ess.partition.memory", "8m").toInt
  private val WORKER_TIMEOUT_MS = conf.getLong("ess.worker.timeout", 60) * 1000
  private val REAPER_ITERATIONS = 15

  // States
  val workers = new util.ArrayList[WorkerInfo]()
  val workersLock = new Object()
  private val idToWorker = new ConcurrentHashMap[String, WorkerInfo]()
  private val addressToWorker = new ConcurrentHashMap[RpcAddress, WorkerInfo]
  private val partitionChunkIndex = new ConcurrentHashMap[String, Int]()
  // key: appid_shuffleid value: resources allocated for the shuffle
  private val shuffleResources = new util.HashMap[String, WorkerResource]()

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
      logDebug(s"received heartbeat from ${workerId}")
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

  def reserveBuffers(slots: WorkerResource): util.List[WorkerInfo] = {
    val failed = new util.ArrayList[WorkerInfo]()

    slots.foreach(entry => {
      val res = entry._1.endpoint.askSync[ReserveBuffersResponse](
        ReserveBuffers(entry._2._1, entry._2._2))
      if (res.success) {
        logInfo(s"Successfully allocated partition buffer from worker ${entry._1.hostPort}")
      } else {
        logInfo(s"Failed to reserve buffers from worker ${entry._1.hostPort}")
        failed.add(entry._1)
      }
    })

    failed
  }

  def reserveBuffersWithRetry(slots: WorkerResource): util.List[WorkerInfo] = {
    // reserve buffers
    var failed = reserveBuffers(slots)

    // retry once if any failed
    failed = if (failed.nonEmpty) {
      logInfo("reserve buffers failed, retry once")
      reserveBuffers(slots.filterKeys(worker => failed.contains(worker)))
    } else null

    failed
  }

  def clearBuffers(slots: WorkerResource): Unit = {
    slots.foreach(entry => {
      val res = entry._1.endpoint.askSync[ClearBuffersResponse](
        ClearBuffers(entry._2._1.map(_.getUUID()), entry._2._2.map(_.getUUID()))
      )
      if (res.success) {
        logInfo(s"ClearBuffers success for worker ${entry._1}")
      } else {
        logInfo(s"Failed to clear buffers for worker ${entry._1}, retry once")
        entry._1.endpoint.ask[ClearBuffersResponse](
          ClearBuffers(entry._2._1.map(_.getUUID()), entry._2._2.map(_.getUUID()))
        )
      }
    })
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions) =>
      logInfo(s"received register shuffle, ${applicationId}, ${shuffleId}, ${numMappers}, ${numPartitions}")
      handleRegisterShuffle(context, applicationId, shuffleId, numPartitions)
    case Revive(applicationId, shuffleId, oldLocation) =>
      logInfo(s"received Revive request, ${applicationId}, ${shuffleId}, ${oldLocation}")
      handleRevive(context, applicationId, shuffleId)
  }

  private def choosePartitionLocation(
      applicationId: String,
      shuffleId: Int,
      reduceId: Int): (WorkerInfo, WorkerInfo) = {
    val available = workers.asScala.filter(w => {
      if (w.freeMemory > PARTITION_MEMORY_MB && w.state == WorkerState.ALIVE) {
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

  def handleRegisterShuffle(context: RpcCallContext, applicationId: String,
    shuffleId: Int, numPartitions: Int): Unit = {
    val shuffleKey = s"${applicationId}-${shuffleId}"

    // check if shuffle has already registered
    if (shuffleResources.containsKey(shuffleKey)) {
      logError(s"shuffle ${shuffleKey} has registered!")
      context.reply(RegisterShuffleResponse(false, null))
      return
    }

    val slots = workers.synchronized {
      MasterUtil.offerSlots(workers, PARTITION_MEMORY_MB, numPartitions)
    }

    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError("offerSlots failed!")
      context.reply(RegisterShuffleResponse(false, null))
      return
    }

    // reserve buffers
    val failed = reserveBuffersWithRetry(slots)

    // reserve buffers failed, clear allocated resources
    if (failed != null && !failed.isEmpty()) {
      logError("reserve buffers still fail after retry, clear buffers")
      clearBuffers(slots)
      logInfo("fail to reserve buffers")
      context.reply(RegisterShuffleResponse(false, null))
      return
    }

    // register shuffle success, update status
    val locations: List[PartitionLocation] = slots.flatMap(_._2._1).toList
    shuffleResources.synchronized {
      val key = s"${applicationId}-${shuffleId}"
      shuffleResources.put(key, slots)
    }
    logInfo(s"going to reply register success, locations ${locations}")
    context.reply(RegisterShuffleResponse(true, locations))
  }

  private def handleRevive(context: RpcCallContext, applicationId: String, shuffleId: Int): Unit = {
    // check whether shuffle has registered
    val shuffleKey = s"${applicationId}-${shuffleId}"
    if (!shuffleResources.containsKey(shuffleKey)) {
      logError(s"shuffle ${shuffleKey} has not registered!")
      context.reply(ReviveResponse(false, null))
      return
    }

    // offer new slot
    val slots = workers.synchronized {
      MasterUtil.offerSlots(workers, PARTITION_MEMORY_MB, 1)
    }
    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError("offerSlot failed!")
      context.reply(ReviveResponse(false, null))
      return
    }

    // reserve buffer
    val failed = reserveBuffersWithRetry(slots)
    // reserve buffers failed, clear allocated resources
    if (failed != null && !failed.isEmpty()) {
      logError("reserve buffers still fail after retry, clear buffers")
      clearBuffers(slots)
      logInfo("fail to reserve buffers")
      context.reply(ReviveResponse(false, null))
      return
    }

    // update status
    shuffleResources.synchronized {
      val resource = shuffleResources.get(shuffleKey)
      val workerInfo = slots.keySet().head
      resource.putIfAbsent(workerInfo,
        (new util.ArrayList[PartitionLocation](), new util.ArrayList[PartitionLocation]()))
      val (masterLocations, slaveLocations) = resource.get(workerInfo)
      masterLocations.addAll(slots.head._2._1)
      slaveLocations.addAll(slots.head._2._2)
    }

    // reply success
    context.reply(ReviveResponse(true, slots.head._2._1.head))
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
