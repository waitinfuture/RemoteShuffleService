package com.aliyun.emr.jss.service.deploy.master

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import com.aliyun.emr.jss.common.EssConf
import com.aliyun.emr.jss.common.internal.Logging
import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import com.aliyun.emr.jss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ControlMessages._
import com.aliyun.emr.jss.protocol.message.ReturnCode
import com.aliyun.emr.jss.service.deploy.worker.WorkerInfo

private[deploy] class Master(
  override val rpcEnv: RpcEnv,
  address: RpcAddress,
  val conf: EssConf)
  extends RpcEndpoint with Logging {

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
  val workers: util.List[WorkerInfo] = new util.ArrayList[WorkerInfo]()
  val workersLock = new Object()
  private val partitionChunkIndex = new ConcurrentHashMap[String, Int]()
  // key: appid_shuffleid
  private val registeredShuffle = new util.HashSet[String]()
  private val shuffleMapperAttempts = new util.HashMap[String, Array[Int]]()
  private val shuffleCommittedPartitions = new util.HashMap[String, util.Set[String]]()
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
      handleHeartBeat(host, port)
    case WorkerLost(host, port) =>
      logInfo(s"received WorkerLost, ${host}:port")
      handleWorkerLost(host, port)
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

  def destroyBuffersWithRetry(shuffleKey: String, worker: WorkerInfo,
    masterLocations: util.List[PartitionLocation], slaveLocations: util.List[PartitionLocation]
  ): (util.List[PartitionLocation], util.List[PartitionLocation]) = {
    var res = worker.endpoint.askSync[DestroyResponse](
      Destroy(shuffleKey, masterLocations, slaveLocations)
    )
    if (res.returnCode != ReturnCode.Success) {
      res = worker.endpoint.askSync[DestroyResponse](
        Destroy(shuffleKey, res.failedMasters, res.failedSlaves)
      )
    }
    (res.failedMasters, res.failedSlaves)
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
    case SlaveLost(shuffleKey, masterLocation, slaveLocation: PartitionLocation) =>
      logInfo(s"receive SlaveLost request, ${slaveLocation}")
      handleSlaveLost(context, shuffleKey, masterLocation, slaveLocation)
    case Trigger(applicationId, shuffleId) =>

      val result = workers(1).endpoint.askSync[CommitFilesResponse](CommitFiles(
        s"${applicationId}-${shuffleId}",
        workers(1).masterPartitionLocations.get(s"${applicationId}-${shuffleId}").keySet().toList,
        PartitionLocation.Mode.Master
      ))

      context.reply(TriggerResponse(true))
    case GetWorkerInfos =>
      logInfo("received GetWorkerInfo request")
      handleGetWorkerInfos(context)
    case StageEnd(applicationId, shuffleId) =>
      logInfo(s"receive StageEnd request, ${applicationId}, ${shuffleId}")
      handleStageEnd(context, applicationId, shuffleId)
  }

  private def timeOutDeadWorkers() {
    val currentTime = System.currentTimeMillis()
    var ind = 0
    while (ind < workers.size()) {
      if (workers.get(ind).lastHeartbeat < currentTime - WORKER_TIMEOUT_MS) {
        logInfo(s"Worker ${workers.get(ind)} timeout! Trigger WorkerLost event")
        // trigger WorkerLost event
        self.send(
          WorkerLost(workers.get(ind).host, workers.get(ind).port)
        )
      } else {
        ind += 1
      }
    }
  }

  private def handleHeartBeat(host: String, port: Int): Unit = {
    logDebug(s"received heartbeat from ${host}:${port}")
    val worker: WorkerInfo = workers.find(w => w.host == host && w.port == port).orNull
    if (worker == null) {
      logInfo(s"received Heartbeart from unknown worker! ${host}:${port}")
      return
    }
    worker.synchronized {
      worker.lastHeartbeat = System.currentTimeMillis()
    }
  }

  private def handleWorkerLost(host: String, port: Int): Unit = {
    // find worker
    val worker: WorkerInfo = workers.find(w => w.host == host && w.port == port).orNull
    if (worker == null) {
      logError(s"Unkonwn worker ${host}:${port} for WorkerLost handler!")
      return
    }
    // remove worker from workers
    workers.synchronized {
      workers.remove(worker)
    }
    // for all master partitions on the lost worker, send CommitFiles to their slave patitions
    // then destroy the slave partitions
    worker.masterPartitionLocations.foreach(entry => {
      val shuffleKey = entry._1
      val slaveLocations = entry._2.keySet().map(_.getPeer)
      val groupedByWorker = slaveLocations.groupBy(loc => loc.hostPort())
      groupedByWorker.foreach(elem => {
        val worker = workers.find(w => w.hostPort == elem._1).orNull
        // commit files
        val res = worker.endpoint.askSync[CommitFilesResponse](
          CommitFiles(shuffleKey, elem._2.toList, PartitionLocation.Mode.Slave)
        )
        // record commited Files
        val committedPartitions = shuffleCommittedPartitions.get(shuffleKey)
        if (res.failedLocations == null || res.failedLocations.isEmpty) {
          committedPartitions.synchronized {
            committedPartitions.addAll(elem._2.map(_.getUUID))
          }
        } else {
          committedPartitions.synchronized {
            committedPartitions.addAll(
              elem._2.filter(p => !res.failedLocations.contains(p)).map(_.getUUID)
            )
          }
        }
        // destroy slave partitions
        val resDestroy = worker.endpoint.askSync[DestroyResponse](
          Destroy(shuffleKey, null, elem._2.toList)
        )
        // retry once to destroy
        if (resDestroy.returnCode != ReturnCode.Success) {
          worker.endpoint.askSync[DestroyResponse](
            Destroy(shuffleKey, null, resDestroy.failedSlaves)
          )
        }
      })
    })

    // for all slave partitions on the lost worker, send SlaveLost to their master locations
    worker.slavePartitionLocations.foreach(entry => {
      val shuffleKey = entry._1
      val masterLocations = entry._2.keySet().map(_.getPeer)
      val groupedByWorker = masterLocations.groupBy(loc => loc.hostPort())
      groupedByWorker.foreach (entry => {
        val worker: WorkerInfo = workers.find(w => w.hostPort == entry._1).orNull
        // send SlaveLost to all master locations
        entry._2.foreach(loc => {
          worker.endpoint.askSync[SlaveLostResponse](
            SlaveLost(shuffleKey, loc, loc.getPeer)
          )
        })
      })
    })

    logInfo("Finished to process WorkerLost!")
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
      logInfo(s"registered worker ${worker}")
      context.reply(RegisterWorkerResponse(true, null))
    }
  }

  def handleRegisterShuffle(context: RpcCallContext, applicationId: String,
    shuffleId: Int, numMappers: Int, numPartitions: Int): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)

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
      slots.foreach(entry => {
        destroyBuffersWithRetry(shuffleKey, entry._1, entry._2._1, entry._2._2)
        entry._1.synchronized {
          entry._1.removeMasterPartition(shuffleKey, entry._2._1)
          entry._1.removeSlavePartition(shuffleKey, entry._2._2)
        }
      })
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
    context.reply(RegisterShuffleResponse(true, locations))
  }

  private def handleRevive(context: RpcCallContext, applicationId: String, shuffleId: Int): Unit = {
    // check whether shuffle has registered
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
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
      slots.foreach(entry => {
        destroyBuffersWithRetry(shuffleKey, entry._1, entry._2._1, entry._2._2)
        entry._1.synchronized {
          entry._1.removeMasterPartition(shuffleKey, entry._2._1)
          entry._1.removeSlavePartition(shuffleKey, entry._2._2)
        }
      })
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
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
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
    shuffleKey: String, masterLocation: PartitionLocation,
    slaveLocation: PartitionLocation): Unit = {
    // find slaveWorker
    val slaveWorker = workers.find(w => w.host == slaveLocation.getHost &&
      w.port == slaveLocation.getPort).orNull
    val masterWorker = workers.find(w => w.host == masterLocation.getHost &&
      w.port == masterLocation.getPort).orNull
    if (masterWorker == null) {
      logError("MasterWorker not found!")
      context.reply(SlaveLostResponse(ReturnCode.MasterPartitionNotFound, null))
      return
    }
    // send Destroy
    val (_, failedSlave) = destroyBuffersWithRetry(shuffleKey, slaveWorker, null, List(slaveLocation))
    if (failedSlave == null || failedSlave.isEmpty) {
      // remove slave partition
      if (slaveWorker != null) {
        slaveWorker.synchronized {
          slaveWorker.removeSlavePartition(shuffleKey, slaveLocation)
        }
      }
      // update master locations's peer
      masterWorker.synchronized {
        masterWorker.masterPartitionLocations.get(shuffleKey).get(masterLocation).setPeer(null)
      }
    }
    // offer new slot
    val location = workers.synchronized {
      MasterUtil.offerSlaveSlot(slaveLocation.getPeer, workers)
    }
    if (location == null) {
      logError("offer slot failed!")
      context.reply(SlaveLostResponse(ReturnCode.SlotNotAvailable, null))
      return
    }
    // reserve buffer
    val slots =
      new util.HashMap[WorkerInfo, (util.List[PartitionLocation], util.List[PartitionLocation])]()
    val locationList = new util.ArrayList[PartitionLocation]()
    locationList.add(location._2)
    slots.put(location._1, (new util.ArrayList[PartitionLocation](), locationList))
    val failed = reserveBuffersWithRetry(shuffleKey, slots)
    if (failed != null && !failed.isEmpty()) {
      logError("reserve buffer failed!")
      // update status
      if (slaveWorker != null) {
        slaveWorker.synchronized {
          slaveWorker.removeSlavePartition(shuffleKey, location._2)
        }
      }
      context.reply(SlaveLostResponse(ReturnCode.ReserveBufferFailed, null))
      return
    }
    // update peer
    masterWorker.synchronized {
      masterWorker.masterPartitionLocations.get(shuffleKey).get(masterLocation).setPeer(location._2)
    }
    // handle SlaveLost success, reply
    context.reply(SlaveLostResponse(ReturnCode.Success, location._2))
  }

  private def handleGetWorkerInfos(context: RpcCallContext): Unit = {
    context.reply(GetWorkerInfosResponse(true, workers))
  }

  private def handleStageEnd(context: RpcCallContext,
    applicationId: String, shuffleId: Int): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    // check whether shuffle has registered
    if (registeredShuffle.contains(shuffleKey)) {
      logInfo(s"shuffle ${shuffleKey} not reigstered!")
      context.reply(StageEndResponse(ReturnCode.ShuffleNotRegistered, null))
      return
    }

    def addCommitedPartitions(failedLocations: util.List[PartitionLocation],
      allLocations: util.Set[PartitionLocation]): Unit = {
      val committedPartitions = shuffleCommittedPartitions.get(shuffleKey)
      if (failedLocations == null) {
        committedPartitions.synchronized {
          committedPartitions.addAll(allLocations.map(_.getUUID))
        }
      } else {
        committedPartitions.synchronized {
          committedPartitions.addAll(
            allLocations.filter(loc => !failedLocations.contains(loc)).map(_.getUUID)
          )
        }
      }
    }

    // ask allLocations workers holding master partitions to commit files
    val failedMasters: util.List[PartitionLocation] = new util.ArrayList[PartitionLocation]()
    workers.foreach(worker => {
      if (worker.masterPartitionLocations.contains(shuffleKey)) {
        val res = worker.endpoint.askSync[CommitFilesResponse](
          CommitFiles(shuffleKey,
            worker.masterPartitionLocations.get(shuffleKey).keySet().toList,
            PartitionLocation.Mode.Master)
        )
        // record failed masters
        if (res.failedLocations != null) {
          failedMasters.addAll(res.failedLocations)
        }
        // record commited partitionIds
        addCommitedPartitions(failedMasters,
          worker.masterPartitionLocations.get(shuffleKey).keySet())
      }
    })

    // if any failedMasters, ask allLocations workers holding slave partitions to commit files
    if (!failedMasters.isEmpty) {
      // group allLocations failedMasters partitions by location
      val grouped = failedMasters.map(_.getPeer).groupBy(loc => loc.hostPort())
      val failedSlaves: util.List[PartitionLocation] = new util.ArrayList[PartitionLocation]()
      grouped.foreach(entry => {
        val worker = workers.find(w => w.hostPort.equals(entry._1)).get
        val slavePartitions = entry._2.toList
        val res = worker.endpoint.askSync[CommitFilesResponse](
          CommitFiles(shuffleKey,
            slavePartitions,
            PartitionLocation.Mode.Master)
        )
        // record failed locations
        if (res.failedLocations != null) {
          failedSlaves.addAll(res.failedLocations)
        }
        // record commited partitionids
        addCommitedPartitions(failedSlaves,
          worker.slavePartitionLocations.get(shuffleKey).keySet())
      })
    }

    // ask all workers holding master/slave partition to release resource
    workers.foreach(worker => {
      val res = worker.endpoint.askSync[DestroyResponse](
        Destroy(shuffleKey,
          worker.masterPartitionLocations.get(shuffleKey).keySet().toList,
          worker.slavePartitionLocations.get(shuffleKey).keySet().toList)
      )
      // retry once to destroy
      if (res.returnCode != ReturnCode.Success) {
        worker.endpoint.askSync[DestroyResponse](
          Destroy(shuffleKey,
            res.failedMasters,
            res.failedSlaves
          )
        )
      }
    })
    // release resources and clear worker info
    workers.synchronized {
      workers.foreach(worker => {
        worker.removeMasterPartition(shuffleKey,
          worker.masterPartitionLocations.get(shuffleKey).keySet().toList)
        worker.removeSlavePartition(shuffleKey,
          worker.slavePartitionLocations.get(shuffleKey).keySet().toList)
        worker.masterPartitionLocations.remove(shuffleKey)
        worker.slavePartitionLocations.remove(shuffleKey)
      })
    }

    // check if committed files contains all files mappers written
    val lostFiles = new util.ArrayList[String]()
    val committedPartitions = shuffleCommittedPartitions.get(shuffleKey)
    shufflePartitionsWritten.get(shuffleKey).foreach(id => {
      if (!committedPartitions.contains(id)) {
        lostFiles.add(id)
      }
    })

    // clear shufflePartitionsWritten for current shuffle
    shufflePartitionsWritten.synchronized {
      shufflePartitionsWritten.remove(shuffleKey)
    }

    // reply
    if (lostFiles.isEmpty) {
      context.reply(StageEndResponse(ReturnCode.Success, null))
    } else {
      context.reply(StageEndResponse(ReturnCode.PartialSuccess, null))
    }
  }

}

private[deploy] object Master
  extends Logging {
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
