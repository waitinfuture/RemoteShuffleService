package com.aliyun.emr.jss.service.deploy.master

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.Breaks._

import com.aliyun.emr.jss.common.EssConf
import com.aliyun.emr.jss.common.internal.Logging
import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEnv}
import com.aliyun.emr.jss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ControlMessages._
import com.aliyun.emr.jss.service.deploy.DeployMessages._

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    val conf: EssConf) extends RpcEndpoint with Logging {

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
  private val CHUNK_SIZE = conf.getSizeAsKb("ess.partition.memory", "32m").toInt
  private val WORKER_TIMEOUT_MS = conf.getLong("ess.worker.timeout", 60) * 1000
  private val REAPER_ITERATIONS = 15

  // States
  val workers = new util.ArrayList[WorkerInfo]()
  val workersLock = new Object()
  private val partitionChunkIndex = new ConcurrentHashMap[String, Int]()
  // key: appid_shuffleid
  private val registeredShuffle = new util.HashSet[String]()
  private val shuffleMapperAttempts = new util.HashMap[String, Array[Int]]()
  private val shufflePartitionsWritten = new util.HashMap[String, util.Set[String]]()

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
    logInfo(s"client $address got disassociated")
  }

  override def receive: PartialFunction[Any, Unit] = {

    case CheckForWorkerTimeOut =>
      timeOutDeadWorkers()

    case Heartbeat(host, port) =>
      logDebug(s"received heartbeat from ${host}:${port}")
      var worker: WorkerInfo = null
      workers.synchronized {
        breakable({
          for (ind <- 0 until workers.size()) {
            if (workers.get(ind).host == host && workers.get(ind).port == port) {
              worker = workers.get(ind)
              break()
            }
          }
        })
      }
      if (worker != null) {
        worker.lastHeartbeat = System.currentTimeMillis()
      }

    case RegisterWorker(host, port, memory, workerRef) =>
      logInfo("Registering worker %s:%d with %s RAM".format(
        host, port, Utils.megabytesToString(memory)))
      if (workers.exists(w => w.host == host && w.port == port)) {
        logError(s"Worker already registered! ${host}:${port}")
        workerRef.send(RegisterWorkerFailed("Duplicate worker ID"))
      } else {
        val worker = new WorkerInfo(host, port, memory, CHUNK_SIZE, workerRef)
        workers.synchronized {
          workers.add(worker)
        }
        logInfo(s"registered worker ${worker}")
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
      handleRegisterShuffle(context, applicationId, shuffleId, numMappers, numPartitions)
    case Revive(applicationId, shuffleId) =>
      logInfo(s"received Revive request, ${applicationId}, ${shuffleId}")
      handleRevive(context, applicationId, shuffleId)
    case OfferSlave(partitionId) =>
      logInfo(s"received OfferSlave request, partitionId: ${partitionId}")
      handleOfferSlave(context, partitionId)
    case MapperEnd(applicationId: String, shuffleId: Int, mapId: Int,
      attemptId: Int, partitionIds: util.List[String]) =>
      logInfo(s"receive MapperEnd request, ${applicationId}, ${shuffleId}, ${mapId}, ${attemptId}")
      handleMapperEnd(context, applicationId, shuffleId, mapId, attemptId, partitionIds)
  }

  private def choosePartitionLocation(
      applicationId: String,
      shuffleId: Int,
      reduceId: Int): (WorkerInfo, WorkerInfo) = {
    val available = workers.asScala.filter(w => {
      if (w.freeMemory > CHUNK_SIZE && w.state == WorkerState.ALIVE) {
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

  private def timeOutDeadWorkers() {
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.asScala
      .filter(w => w.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS)
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning(s"Removing ${worker} because of timeout for ${WORKER_TIMEOUT_MS / 1000} seconds")
        removeWorker(worker, s"Not receiving heartbeat for ${WORKER_TIMEOUT_MS / 1000} seconds")
      } else {
        if (worker.lastHeartbeat < currentTime - ((REAPER_ITERATIONS + 1) * WORKER_TIMEOUT_MS)) {
          workers.remove(worker)
        }
      }
    }
  }

  private def removeWorker(worker: WorkerInfo, msg: String) {
    logInfo(s"Removing worker ${worker}")
    worker.setState(WorkerState.DEAD)
  }

  def handleRegisterShuffle(context: RpcCallContext, applicationId: String,
    shuffleId: Int, numMappers: Int, numPartitions: Int): Unit = {
    val shuffleKey = s"${applicationId}-${shuffleId}"

    // check if shuffle has already registered
    if (registeredShuffle.contains(shuffleKey)) {
      logError(s"shuffle ${shuffleKey} has registered!")
      context.reply(RegisterShuffleResponse(false, null))
      return
    }

    val slots = workers.synchronized {
      MasterUtil.offerSlots(workers, CHUNK_SIZE, numPartitions)
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
    workers.synchronized {
      slots.foreach(entry => {
        entry._1.addMasterPartition()
      })
      shuffleResources.put(shuffleKey, slots)
    }
    shuffleMapperAttempts.synchronized {
      val attempts = new Array[Int](numMappers)
      0 until numMappers foreach (idx => attempts(idx) = 0)
      shuffleMapperAttempts.put(shuffleKey, attempts)
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
      MasterUtil.offerSlots(workers, CHUNK_SIZE, 1)
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

  private def handleOfferSlave(context: RpcCallContext, partitionId: String): Unit = {
    // offer slot
    val location = workers.synchronized {
      MasterUtil.offerSlot(partitionId, workers, CHUNK_SIZE)
    }
    if (location == null) {
      logError("offer slot failed!");
      context.reply(OfferSlaveResponse(false, null))
      return
    }

    // reserve buffer
    val slots = new util.HashMap[WorkerInfo, (util.List[PartitionLocation], util.List[PartitionLocation])]()
    val locationList = new util.ArrayList[PartitionLocation]()
    locationList.add(location._2)
    slots.put(location._1, (new util.ArrayList[PartitionLocation](), locationList))
    val failed = reserveBuffersWithRetry(slots)
    if (failed != null && !failed.isEmpty()) {
      logError("reserve buffer failed!")
      context.reply(OfferSlaveResponse(false, null))
      return
    }

    // offer slave success
    logInfo(s"offer slave success, partition location ${location._2}")
    context.reply(OfferSlaveResponse(true, location._2))
  }

  private def handleMapperEnd(
    context: RpcCallContext,
    applicationId: String,
    shuffleId: Int,
    mapId: Int,
    attemptId: Int,
    partitionIds: util.List[String]
  ): Unit = {
    val shuffleKey = s"${applicationId}-${shuffleId}"
    // update max attemptId
    shuffleMapperAttempts.synchronized {
      val attempts = shuffleMapperAttempts.get(shuffleKey)
      if (attempts == null) {
        logError(s"shuffle not registered! ${shuffleKey}")
        context.reply(MapperEndResponse(false))
        return
      }
      if (attempts(mapId) < attemptId) {
        attempts(mapId) = attemptId
      }
    }

    // update partitions written
    shufflePartitionsWritten.synchronized {
      shufflePartitionsWritten.putIfAbsent(shuffleKey, new util.HashSet[String]())
      val partitionsWritten = shufflePartitionsWritten.get(shuffleKey)
      partitionsWritten.addAll(partitionIds)
    }

    // reply success
    context.reply(MapperEndResponse(true))
  }

  private def handleSlaveLost(context: RpcCallContext,
    partitionLocation: PartitionLocation): Unit = {
    // find worker
    var worker: WorkerInfo = null
    breakable({
      for (ind <- 0 until workers.length) {
        if (workers.get(ind).host == partitionLocation.getHost
          && workers.get(ind).port == partitionLocation.getPort) {
          worker = workers.get(ind)
          break()
        }
      }
    })
    if (worker == null) {
      logError("Worker not found!")
      context.reply(SlaveLostResponse(false))
      return
    }

    // send ClearBuffer
    val res = worker.endpoint.askSync[ClearBuffersResponse](
      ClearBuffers(null, List(partitionLocation.getUUID))
    )
    if (res.success) {
      worker.removeSlavePartition(partitionLocation)
      context.reply(SlaveLostResponse(success))
    }
  }

}

private[deploy] object Master extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new EssConf()
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
