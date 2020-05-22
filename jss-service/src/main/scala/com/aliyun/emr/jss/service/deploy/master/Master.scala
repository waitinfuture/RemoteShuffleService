package com.aliyun.emr.jss.service.deploy.master

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

import com.aliyun.emr.jss.common.EssConf
import com.aliyun.emr.jss.common.internal.Logging
import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import com.aliyun.emr.jss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ControlMessages._
import com.aliyun.emr.jss.service.deploy.worker.WorkerInfo

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
  private val CHUNK_SIZE = conf.getSizeAsBytes("ess.partition.memory", "32m")
  private val WORKER_TIMEOUT_MS = conf.getLong("ess.worker.timeout", 60) * 1000

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
      workers.synchronized {
        breakable({
          for (ind <- 0 until workers.size()) {
            if (workers.get(ind).host == host && workers.get(ind).port == port) {
              workers.get(ind).lastHeartbeat = System.currentTimeMillis()
              break()
            }
          }
        })
      }
  }

  def reserveBuffers(shuffleKey: String, slots: WorkerResource): util.List[WorkerInfo] = {
    val failed = new util.ArrayList[WorkerInfo]()

    slots.foreach(entry => {
      val res = entry._1.endpoint.askSync[ReserveBuffersResponse](
        ReserveBuffers(shuffleKey, entry._2._1, entry._2._2))
      if (res.success) {
        logInfo(s"Successfully allocated partition buffer from worker ${entry._1.hostPort}")
      } else {
        logInfo(s"Failed to reserve buffers from worker ${entry._1.hostPort}")
        failed.add(entry._1)
      }
    })

    failed
  }

  def reserveBuffersWithRetry(shuffleKey: String, slots: WorkerResource): util.List[WorkerInfo] = {
    // reserve buffers
    var failed = reserveBuffers(shuffleKey, slots)

    // retry once if any failed
    failed = if (failed.nonEmpty) {
      logInfo("reserve buffers failed, retry once")
      reserveBuffers(shuffleKey, slots.filterKeys(worker => failed.contains(worker)))
    } else null

    failed
  }

  def clearBuffers(shuffleKey: String, slots: WorkerResource): Unit = {
    slots.foreach(entry => {
      var res = entry._1.endpoint.askSync[ClearBuffersResponse](
        ClearBuffers(shuffleKey, entry._2._1, entry._2._2)
      )
      if (res.success) {
        logInfo(s"ClearBuffers success for worker ${entry._1}")
        // update status
        workers.synchronized {
          entry._1.removeMasterPartition(shuffleKey, entry._2._1)
          entry._1.removeSlavePartition(shuffleKey, entry._2._2)
        }
      } else {
        // retry once
        logInfo(s"Failed to clear buffers for worker ${entry._1}, retry once")
        res = entry._1.endpoint.askSync[ClearBuffersResponse](
          ClearBuffers(shuffleKey, entry._2._1, entry._2._2)
        )
        if (res.success) {
          logInfo(s"ClearBuffers success for worker ${entry._1}")
          // update status
          workers.synchronized {
            entry._1.removeMasterPartition(shuffleKey, entry._2._1)
            entry._1.removeSlavePartition(shuffleKey, entry._2._2)
          }
        }
      }
    })
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterWorker(host, port, memory, worker) =>
      logInfo(s"received RegisterWorker, ${host}:${port} ${memory}")
      handleRegisterWorker(context, host, port, memory, worker)
    case RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions) =>
      logInfo(s"received register shuffle, ${applicationId}, ${shuffleId}, ${numMappers}, ${numPartitions}")
      handleRegisterShuffle(context, applicationId, shuffleId, numMappers, numPartitions)
    case Revive(applicationId, shuffleId) =>
      logInfo(s"received Revive request, ${applicationId}, ${shuffleId}")
      handleRevive(context, applicationId, shuffleId)
    case MapperEnd(applicationId: String, shuffleId: Int, mapId: Int,
      attemptId: Int, partitionIds: util.List[String]) =>
      logInfo(s"receive MapperEnd request, ${applicationId}, ${shuffleId}, ${mapId}, ${attemptId}")
      handleMapperEnd(context, applicationId, shuffleId, mapId, attemptId, partitionIds)
    case SlaveLost(shuffleKey, slaveLocation: PartitionLocation) =>
      logInfo(s"receive SlaveLost request, ${slaveLocation}")
      handleSlaveLost(context, shuffleKey, slaveLocation)
  }

  private def timeOutDeadWorkers() {
    val currentTime = System.currentTimeMillis()
    var ind = 0
    while (ind < workers.size()) {
      if (workers.get(ind).lastHeartbeat < currentTime - WORKER_TIMEOUT_MS) {
        logInfo(s"Worker ${workers.get(ind)} timeout! Trigger WorkerLost event")
        // trigger WorkerLost event
        self.ask[WorkerLostResponse](
          WorkerLost(workers.get(ind).host, workers.get(ind).port)
        )
      } else {
        ind += 1
      }
    }
  }

  def handleRegisterWorker(context: RpcCallContext, host: String, port: Int,
    memory: Long, workerRef: RpcEndpointRef): Unit = {
    logInfo("Registering worker %s:%d with %s RAM".format(
      host, port, Utils.bytesToString(memory)))
    if (workers.exists(w => w.host == host && w.port == port)) {
      logError(s"Worker already registered! ${host}:${port}")
      context.reply(RegisterWorkerResponse(false, "Worker already registered!"))
    } else {
      val worker = new WorkerInfo(host, port, memory, CHUNK_SIZE, workerRef)
      workers.synchronized {
        workers.add(worker)
      }
      context.reply(RegisterWorkerResponse(true, null))
      logInfo(s"registered worker ${worker}")
    }
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
      MasterUtil.offerSlots(shuffleKey, workers, numPartitions)
    }

    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError("offerSlots failed!")
      context.reply(RegisterShuffleResponse(false, null))
      return
    }

    // reserve buffers
    val failed = reserveBuffersWithRetry(shuffleKey, slots)

    // reserve buffers failed, clear allocated resources
    if (failed != null && !failed.isEmpty()) {
      logError("reserve buffers still fail after retry, clear buffers")
      clearBuffers(shuffleKey, slots)
      logInfo("fail to reserve buffers")
      context.reply(RegisterShuffleResponse(false, null))
      return
    }

    // register shuffle success, update status
    registeredShuffle.synchronized {
      registeredShuffle.add(shuffleKey)
    }
    val locations: List[PartitionLocation] = slots.flatMap(_._2._1).toList
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
    if (!registeredShuffle.contains(shuffleKey)) {
      logError(s"shuffle ${shuffleKey} has not registered!")
      context.reply(ReviveResponse(false, null))
      return
    }

    // offer new slot
    val slots = workers.synchronized {
      MasterUtil.offerSlots(shuffleKey, workers, 1)
    }
    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError("offerSlot failed!")
      context.reply(ReviveResponse(false, null))
      return
    }

    // reserve buffer
    val failed = reserveBuffersWithRetry(shuffleKey, slots)
    // reserve buffers failed, clear allocated resources
    if (failed != null && !failed.isEmpty()) {
      clearBuffers(shuffleKey, slots)
      logInfo("fail to reserve buffers")
      context.reply(ReviveResponse(false, null))
      return
    }

    // reply success
    context.reply(ReviveResponse(true, slots.head._2._1.head))
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
    shuffleKey: String, slaveLocation: PartitionLocation): Unit = {
    // find worker
    var worker: WorkerInfo = null
    breakable({
      for (ind <- 0 until workers.length) {
        if (workers.get(ind).host == slaveLocation.getHost
          && workers.get(ind).port == slaveLocation.getPort) {
          worker = workers.get(ind)
          break()
        }
      }
    })
    if (worker == null) {
      logError("Worker not found!")
    } else {
      // send ClearBuffer
      val locs = new util.ArrayList[PartitionLocation]()
      locs.add(slaveLocation)
      var res = worker.endpoint.askSync[ClearBuffersResponse](
        ClearBuffers(shuffleKey, null, locs)
      )
      if (res.success) {
        logInfo("Successfully cleared buffer")
        // update status
        workers.synchronized {
          worker.removeSlavePartition(shuffleKey, slaveLocation)
        }
      } else {
        // retry once
        res = worker.endpoint.askSync[ClearBuffersResponse](
          ClearBuffers(shuffleKey, null, locs)
        )
        if (res.success) {
          logInfo("Successfully cleared buffer")
          // update status
          workers.synchronized {
            worker.removeSlavePartition(shuffleKey, slaveLocation)
          }
        }
      }
    }

    // offer new slot
    val location = workers.synchronized {
      MasterUtil.offerSlaveSlot(slaveLocation.getPeer, workers)
    }
    if (location == null) {
      logError("offer slot failed!")
      context.reply(SlaveLostResponse(false, null))
      return
    }

    // reserve buffer
    val slots = new util.HashMap[WorkerInfo, (util.List[PartitionLocation], util.List[PartitionLocation])]()
    val locationList = new util.ArrayList[PartitionLocation]()
    locationList.add(location._2)
    slots.put(location._1, (new util.ArrayList[PartitionLocation](), locationList))
    val failed = reserveBuffersWithRetry(shuffleKey, slots)
    if (failed != null && !failed.isEmpty()) {
      logError("reserve buffer failed!")
      // update status
      workers.synchronized {
        worker.removeSlavePartition(shuffleKey, location._2)
      }
      context.reply(SlaveLostResponse(false, null))
      return
    }

    // handle SlaveLost success, reply
    context.reply(SlaveLostResponse(true, location._2))
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
