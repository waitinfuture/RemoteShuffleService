package com.aliyun.emr.jss.service.deploy.master

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import com.aliyun.emr.jss.common.EssConf
import com.aliyun.emr.jss.common.internal.Logging
import com.aliyun.emr.jss.common.rpc._
import com.aliyun.emr.jss.common.util.{EssPathUtil, ThreadUtils, Utils}
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ControlMessages._
import com.aliyun.emr.jss.protocol.message.StatusCode
import com.aliyun.emr.jss.service.deploy.worker.WorkerInfo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

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
  private var checkForApplicationTimeOutTask: ScheduledFuture[_] = _
  private val registerShufflePartitionThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "register-shuffle-partition-threadpool",
    8
  )

  // file system
  val basePath = EssPathUtil.GetBaseDir(conf)
  val fs = basePath.getFileSystem(new Configuration())

  // Configs
  private val CHUNK_SIZE = conf.getSizeAsBytes("ess.partition.memory", "32m")
  private val WORKER_TIMEOUT_MS = conf.getLong("ess.worker.timeout", 60) * 1000
  private val APPLICATION_TIMEOUT_MS = conf.getLong("ess.application.timeout", 180) * 1000

  // States
  val workers: util.List[WorkerInfo] = new util.ArrayList[WorkerInfo]()
  val workersLock = new Object()
  private val partitionChunkIndex = new ConcurrentHashMap[String, Int]()
  // key: appid_shuffleid
  private val registeredShuffle = new util.HashSet[String]()
  private val shuffleMapperAttempts = new util.HashMap[String, Array[Int]]()
  private val shuffleCommittedPartitions = new util.HashMap[String, util.Set[String]]()
  private val reducerFileGroup = new util.HashMap[String, util.Set[Path]]()
  private val appHeartbeatTime = new util.HashMap[String, Long]()

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

  // start threads to check timeout for workers and applications
  override def onStart(): Unit = {
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)

    checkForApplicationTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForApplicationTimeOut)
      }
    }, 0, APPLICATION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
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
      timeoutDeadWorkers()
    case CheckForApplicationTimeOut =>
      timeoutDeadApplications()
    case HeartbeatFromWorker(host, port) =>
      handleHeartBeatFromWorker(host, port)
    case WorkerLost(host, port) =>
      logInfo(s"received WorkerLost, ${host}:port")
      handleWorkerLost(null, host, port)
    case HeartBeatFromApplication(appId) =>
      handleHeartBeatFromApplication(appId)
  }

  def reserveBuffers(shuffleKey: String, slots: WorkerResource): util.List[WorkerInfo] = {
    val failed = new util.ArrayList[WorkerInfo]()

    slots.foreach(entry => {
      val res = entry._1.endpoint.askSync[ReserveBuffersResponse](
        ReserveBuffers(shuffleKey, entry._2._1, entry._2._2))
      if (res.status.equals(StatusCode.Success)) {
        logInfo(s"Successfully allocated partitions buffer from worker ${entry._1.hostPort}")
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
    if (res.status != StatusCode.Success) {
      res = worker.endpoint.askSync[DestroyResponse](
        Destroy(shuffleKey, res.failedMasters, res.failedSlaves)
      )
    }
    (res.failedMasters, res.failedSlaves)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterWorker(host, port, memory, worker) =>
      logInfo(s"received RegisterWorker request, $host:$port $memory")
      handleRegisterWorker(context, host, port, memory, worker)

    case RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions) =>
      logInfo(s"received RegisterShuffle request, $applicationId, $shuffleId, $numMappers, $numPartitions")
      handleRegisterShuffle(context, applicationId, shuffleId, numMappers, numPartitions)

    case Revive(applicationId, shuffleId, reduceId) =>
      logInfo(s"received Revive request, $applicationId, $shuffleId, $reduceId")
      handleRevive(context, applicationId, shuffleId, reduceId)

    case MapperEnd(applicationId, shuffleId, mapId, attemptId, partitionLocations) =>
      logInfo(s"received MapperEnd request, $applicationId, $shuffleId, $mapId, $attemptId")
      handleMapperEnd(context, applicationId, shuffleId, mapId, attemptId, partitionLocations)

    case GetReducerFileGroup(applicationId: String, shuffleId: Int) =>
      logInfo(s"received GetShuffleFileGroup request, $applicationId, $shuffleId")
      handleGetReducerFileGroup(context, applicationId, shuffleId)

    case SlaveLost(shuffleKey, masterLocation, slaveLocation: PartitionLocation) =>
      logInfo(s"received SlaveLost request, $slaveLocation")
      handleSlaveLost(context, shuffleKey, masterLocation, slaveLocation)

    case GetWorkerInfos =>
      logInfo("received GetWorkerInfos request")
      handleGetWorkerInfos(context)

    case StageEnd(applicationId, shuffleId) =>
      logInfo(s"received StageEnd request, $applicationId, $shuffleId")
      handleStageEnd(context, applicationId, shuffleId)

    case WorkerLost(host, port) =>
      logInfo(s"received WorkerLost request, $host:$port")
      handleWorkerLost(context, host, port)

    case MasterPartitionSuicide(shuffleKey, location) =>
      logInfo(s"received MasterPartitionSuicide request, $location")
      handleMasterPartitionSuicide(context, shuffleKey, location)

    case UnregisterShuffle(appId, shuffleId) =>
      logInfo(s"received UnregisterShuffle request, $appId-$shuffleId")
      handleUnregisterShuffle(context, appId, shuffleId)

    case ApplicationLost(appId) =>
      logInfo(s"received ApplicationLost request, $appId")
      handleApplicationLost(context, appId)
  }

  private def timeoutDeadWorkers() {
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

  private def timeoutDeadApplications(): Unit = {
    val currentTime = System.currentTimeMillis()
    val keys = appHeartbeatTime.keySet()
    keys.foreach(key => {
      if (appHeartbeatTime.get(key) < currentTime - APPLICATION_TIMEOUT_MS) {
        logError(s"Application ${key} timeout! Trigger ApplicationLost event")
        self.send(
          ApplicationLost(key)
        )
        appHeartbeatTime.remove(key)
      }
    })
  }

  private def handleHeartBeatFromWorker(host: String, port: Int): Unit = {
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

  private def handleWorkerLost(context: RpcCallContext, host: String, port: Int): Unit = {
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
        logInfo("recorded committed files")
        // destroy slave partitions
        val resDestroy = worker.endpoint.askSync[DestroyResponse](
          Destroy(shuffleKey, null, elem._2.toList)
        )
        // retry once to destroy
        if (resDestroy.status != StatusCode.Success) {
          worker.endpoint.askSync[DestroyResponse](
            Destroy(shuffleKey, null, resDestroy.failedSlaves)
          )
        }
        // remove slave partitions
        worker.synchronized {
          worker.removeSlavePartition(shuffleKey, elem._2.toList)
        }
      })
    })

    logInfo("send SlaveLost to all master locations")
    // for all slave partitions on the lost worker, send SlaveLost to their master locations
    worker.slavePartitionLocations.foreach(entry => {
      val shuffleKey = entry._1
      val masterLocations = entry._2.keySet().map(_.getPeer)
      val groupedByWorker = masterLocations.groupBy(loc => loc.hostPort())
      groupedByWorker.foreach(entry => {
        val worker: WorkerInfo = workers.find(w => w.hostPort == entry._1).orNull
        // send SlaveLost to all master locations
        entry._2.foreach(loc => {
          worker.endpoint.send(SlaveLost(shuffleKey, loc, loc.getPeer))
        })
      })
    })

    logInfo("Finished to process WorkerLost!")
    if (context != null) {
      context.reply(WorkerLostResponse(true))
    }
  }

  def handleMasterPartitionSuicide(context: RpcCallContext,
    shuffleKey: String, location: PartitionLocation): Unit = {
    val worker: WorkerInfo = workers.find(w => w.hostPort == location.hostPort()).orNull
    if (worker == null) {
      logError(s"worker not found for the location ${location} !")
      context.reply(MasterPartitionSuicideResponse(StatusCode.WorkerNotFound))
      return
    }
    if (!worker.masterPartitionLocations.contains(shuffleKey)) {
      logError(s"shuffle ${shuffleKey} not registered!")
      context.reply(MasterPartitionSuicideResponse(StatusCode.ShuffleNotRegistered))
      return
    }
    if (!worker.masterPartitionLocations.get(shuffleKey).contains(location)) {
      logError(s"location ${location} not found!")
      context.reply(MasterPartitionSuicideResponse(StatusCode.MasterPartitionNotFound))
      return
    }
    worker.synchronized {
      worker.removeMasterPartition(shuffleKey, location)
    }
    context.reply(MasterPartitionSuicideResponse(StatusCode.Success))
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
    if (registeredShuffle.contains(shuffleKey)) {
      logError(s"shuffle $shuffleKey already registered!")
      context.reply(RegisterShuffleResponse(StatusCode.ShuffleAlreadyRegistered, null))
      return
    }

    val slots = workers.synchronized {
      MasterUtil.offerSlots(shuffleKey, workers, (0 until numPartitions).map(new Integer(_)).toList)
    }

    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError("offerSlots failed!")
      context.reply(RegisterShuffleResponse(StatusCode.SlotNotAvailable, null))
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
      context.reply(RegisterShuffleResponse(StatusCode.ReserveBufferFailed, null))
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
    shuffleCommittedPartitions.synchronized {
      val commitedPartitions = new util.HashSet[String]()
      shuffleCommittedPartitions.put(shuffleKey, commitedPartitions)
    }

    // TODO 维护 reduceGroup ???
    context.reply(RegisterShuffleResponse(StatusCode.Success, locations))
  }

  private def handleRevive(context: RpcCallContext, applicationId: String, shuffleId: Int, reduceId: Int): Unit = {
    // check whether shuffle has registered
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    if (!registeredShuffle.contains(shuffleKey)) {
      logError(s"shuffle $shuffleKey already registered!")
      context.reply(ReviveResponse(StatusCode.ShuffleAlreadyRegistered, null))
      return
    }

    // offer new slot
    val slots = workers.synchronized {
      MasterUtil.offerSlots(shuffleKey, workers, Seq(new Integer(reduceId)))
    }
    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError("offerSlot failed!")
      context.reply(ReviveResponse(StatusCode.SlotNotAvailable, null))
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
      context.reply(ReviveResponse(StatusCode.ReserveBufferFailed, null))
      return
    }

    // reply success
    val (masters, slaves) = slots.head._2
    if (masters != null && masters.size() > 0) {
      context.reply(ReviveResponse(StatusCode.Success, masters.head))
    } else {
      context.reply(ReviveResponse(StatusCode.Success, slaves.head.getPeer))
    }
  }

  private def handleMapperEnd(context: RpcCallContext,
    applicationId: String,
    shuffleId: Int,
    mapId: Int,
    attemptId: Int,
    partitionLocations: util.Set[PartitionLocation]): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    // update max attemptId
    shuffleMapperAttempts.synchronized {
      val attempts = shuffleMapperAttempts.get(shuffleKey)
      if (attempts == null) {
        logError(s"shuffle $shuffleKey not registered!")
        context.reply(MapperEndResponse(StatusCode.ShuffleNotRegistered))
        return
      }

      if (attempts(mapId) < attemptId) {
        attempts(mapId) = attemptId
      }
    }

    // update partitions written
    reducerFileGroup.synchronized {
      partitionLocations
        .map(loc => {
          val partitionKey = Utils.makeReducerKey(applicationId, shuffleId, loc.getReduceId)
          val path = EssPathUtil.GetPartitionPath(conf, applicationId, shuffleId, loc.getReduceId, loc.getUUID)
          (partitionKey, path)
        })
        .groupBy(_._1) // Map[String,List[(String, Path)]]
        .mapValues(r => {r.map(r => {r._2})}) // Map[String, List[Path]]
        .foreach(entry => {
          reducerFileGroup.putIfAbsent(entry._1, new util.HashSet[Path]())
          reducerFileGroup.get(entry._1).addAll(entry._2)
        })
    }

    // reply success
    context.reply(MapperEndResponse(StatusCode.Success))
  }

  private def handleGetReducerFileGroup(context: RpcCallContext,
    applicationId: String,
    shuffleId: Int): Unit = {

    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    val shuffleFileGroup = reducerFileGroup
      .filter(entry => entry._1.startsWith(shuffleKey))
      .map(entry => (entry._1, new util.HashSet(entry._2.map(_.toString).asJava)))
      .toMap

    context.reply(GetReducerFileGroupResponse(
      StatusCode.Success,
      new util.HashMap(shuffleFileGroup.asJava),
      shuffleMapperAttempts.get(shuffleKey)
    ))
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
      context.reply(SlaveLostResponse(StatusCode.MasterPartitionNotFound, null))
      return
    }
    // send Destroy
    if (slaveWorker != null) {
      val (_, failedSlave) = destroyBuffersWithRetry(shuffleKey, slaveWorker, null, List(slaveLocation))
      if (failedSlave == null || failedSlave.isEmpty) {
        // remove slave partition
        if (slaveWorker != null) {
          slaveWorker.synchronized {
            slaveWorker.removeSlavePartition(shuffleKey, slaveLocation)
          }
        }
      }
    }
    // update master locations's peer
    masterWorker.synchronized {
      masterWorker.masterPartitionLocations.get(shuffleKey).get(masterLocation).setPeer(null)
    }
    // offer new slot
    val location = workers.synchronized {
      MasterUtil.offerSlaveSlot(slaveLocation.getPeer, workers)
    }
    if (location == null) {
      logError("offer slot failed!")
      context.reply(SlaveLostResponse(StatusCode.SlotNotAvailable, null))
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
      context.reply(SlaveLostResponse(StatusCode.ReserveBufferFailed, null))
      return
    }
    // add slave partition
    location._1.synchronized {
      location._1.addSlavePartition(shuffleKey, location._2)
    }
    // update peer
    masterWorker.synchronized {
      masterWorker.masterPartitionLocations.get(shuffleKey).get(masterLocation).setPeer(location._2)
    }
    // handle SlaveLost success, reply
    context.reply(SlaveLostResponse(StatusCode.Success, location._2))
  }

  private def handleGetWorkerInfos(context: RpcCallContext): Unit = {
    context.reply(GetWorkerInfosResponse(StatusCode.Success, workers))
  }

  private def handleStageEnd(context: RpcCallContext,
    applicationId: String, shuffleId: Int): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    // check whether shuffle has registered
    if (!registeredShuffle.contains(shuffleKey)) {
      logInfo(s"shuffle ${shuffleKey} not reigstered!")
      context.reply(StageEndResponse(StatusCode.ShuffleNotRegistered, null))
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
      if (res.status != StatusCode.Success) {
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
    reducerFileGroup.filter(entry => entry._1.startsWith(shuffleKey))
        .foreach(entry => {
          val paths = entry._2
          paths.foreach(path => {
            if (!committedPartitions.contains(path.getName)) {
              lostFiles.add(path.getName)
            }
          })
        })

    // clear committed files
    shuffleCommittedPartitions.synchronized {
      shuffleCommittedPartitions.remove(shuffleKey)
    }

    // reply
    if (lostFiles.isEmpty) {
      context.reply(StageEndResponse(StatusCode.Success, null))
    } else {
      context.reply(StageEndResponse(StatusCode.PartialSuccess, null))
    }
  }

  def handleUnregisterShuffle(context: RpcCallContext,
    appId: String, shuffleId: Int): Unit = {
    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
    // if PartitionLocations exists for the shuffle, return fail
    if (partitionExists(shuffleKey)) {
      logError(s"Partition exists for shuffle ${shuffleKey}!")
      context.reply(UnregisterShuffleResponse(StatusCode.PartitionExists))
      return
    }
    // clear reducerFileGroup for the shuffle
    reducerFileGroup.synchronized {
      val keys = reducerFileGroup.keySet()
      keys.filter(key => key.startsWith(shuffleKey)).foreach(key => reducerFileGroup.remove(key))
    }
    // delete shuffle files
    val shuffleDir = EssPathUtil.GetShuffleDir(conf, appId, shuffleId)
    val success = fs.delete(shuffleDir, true)
    if (success) {
      context.reply(UnregisterShuffleResponse(StatusCode.Success))
    } else {
      logError("delete files failed!")
      context.reply(UnregisterShuffleResponse(StatusCode.DeleteFilesFailed))
    }
  }

  def handleApplicationLost(context: RpcCallContext, appId: String): Unit = {
    def getNextShufflePartitions(worker: WorkerInfo):
    (String, util.List[PartitionLocation], util.List[PartitionLocation]) = {
      val shuffleKeys = new util.HashSet[String]()
      shuffleKeys.addAll(worker.masterPartitionLocations.keySet())
      shuffleKeys.addAll(worker.slavePartitionLocations.keySet())
      val nextShuffleKey = shuffleKeys.find(_.startsWith(appId)).orNull
      if (nextShuffleKey == null) {
        return null
      }

      (nextShuffleKey, worker.masterPartitionLocations.get(nextShuffleKey).keySet().toList,
        worker.slavePartitionLocations.get(nextShuffleKey).keySet().toList)
    }

    // destroy partition buffers in workers, then update info in master
    workers.foreach(worker => {
      var nextShufflePartitions = getNextShufflePartitions(worker)
      while(nextShufflePartitions != null) {
        if (nextShufflePartitions != null) {
          val (shuffleKey, masterLocs, slaveLocs) = nextShufflePartitions
          // destroy partition buffers on worker
          val res = worker.endpoint.askSync[DestroyResponse](
            Destroy(shuffleKey, masterLocs, slaveLocs)
          )
          // retry once
          if (res.status != StatusCode.Success) {
            worker.endpoint.askSync[DestroyResponse](
              Destroy(shuffleKey, res.failedMasters, res.failedSlaves)
            )
          }
          // remove partitions from workerinfo
          worker.synchronized {
            worker.removeMasterPartition(shuffleKey, masterLocs)
            worker.removeSlavePartition(shuffleKey, slaveLocs)
          }
        }
        nextShufflePartitions = getNextShufflePartitions(worker)
      }
    })
    // clear reducerFileGroup for the application
    reducerFileGroup.synchronized {
      val keys = reducerFileGroup.keySet()
      keys.filter(key => key.startsWith(appId)).foreach(key => reducerFileGroup.remove(key))
    }
    // delete files for the application
    val appPath = EssPathUtil.GetAppDir(conf, appId)
    if (fs.delete(appPath, true)) {
      logInfo("Finished handling ApplicationLost")
      context.reply(ApplicationLostResponse(true))
    } else {
      logError("Delete files failed!")
      context.reply(ApplicationLostResponse(false))
    }
  }

  private def handleHeartBeatFromApplication(appId: String): Unit = {
    appHeartbeatTime.synchronized {
      appHeartbeatTime.put(appId, System.currentTimeMillis())
    }
  }

  private def partitionExists(shuffleKey: String): Boolean = {
    workers.exists(w => {
      w.masterPartitionLocations.contains(shuffleKey) ||
        w.slavePartitionLocations.contains(shuffleKey)
    })
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
