package com.aliyun.emr.ess.service.deploy.master

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConversions._

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.rpc._
import com.aliyun.emr.ess.common.rpc.netty.NettyRpcEndpointRef
import com.aliyun.emr.ess.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.ess.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.ess.protocol.message.ControlMessages._
import com.aliyun.emr.ess.protocol.message.StatusCode
import com.aliyun.emr.ess.service.deploy.master.http.HttpServer
import com.aliyun.emr.ess.service.deploy.worker.WorkerInfo
import io.netty.util.internal.ConcurrentSet

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

  // Configs
  private val WORKER_TIMEOUT_MS = EssConf.essWorkerTimeoutMs(conf)
  private val APPLICATION_TIMEOUT_MS = EssConf.essApplicationTimeoutMs(conf)

  // States
  val workers: util.List[WorkerInfo] = new util.ArrayList[WorkerInfo]()
  val workersLock = new Object()

  // key: "appId_shuffleId"
  private val registeredShuffle = new util.HashSet[String]()
  private val shuffleMapperAttempts = new util.HashMap[String, Array[Int]]()
  private val shuffleCommittedPartitions =
    new ConcurrentHashMap[String, util.Set[PartitionLocation]]()
  private val reducerFileGroupsMap =
    new ConcurrentHashMap[String, Array[Array[PartitionLocation]]]()
  private val appHeartbeatTime = new util.HashMap[String, Long]()
  private val stageEndShuffleSet = new util.HashSet[String]()

  // revive request waiting for response
  private val reviving =
    new util.HashMap[String, util.HashMap[PartitionLocation, util.Set[RpcCallContext]]]()

  // register shuffle request waiting for response
  private val registerShuffleRequest = new util.HashMap[String, util.Set[RpcCallContext]]()

  // blacklist
  private val blacklist = new ConcurrentSet[String]()

  // workerLost events
  private val workerLostEvents = new ConcurrentSet[String]()

  // http server
  val httpServer = new HttpServer(9098, this)
  httpServer.start()
  logInfo("[Master] httpServer started")

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
    }, 0, APPLICATION_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS)
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
      logInfo(s"received WorkerLost, $host:$port")
      handleWorkerLost(null, host, port)
    case HeartBeatFromApplication(appId) =>
      handleHeartBeatFromApplication(appId)
    case StageEnd(applicationId, shuffleId) =>
      logInfo(s"received StageEnd request, $applicationId, $shuffleId")
      handleStageEnd(null, applicationId, shuffleId)
  }

  def reserveBuffers(
      applicationId: String, shuffleId: Int, slots: WorkerResource): util.List[WorkerInfo] = {
    val failed = new util.ArrayList[WorkerInfo]()

    slots.foreach(entry => {
      val res = entry._1.endpoint.askSync[ReserveBuffersResponse](
        ReserveBuffers(applicationId, shuffleId, entry._2._1, entry._2._2))
      if (res.status.equals(StatusCode.Success)) {
        logInfo(s"Successfully allocated partitions buffer from worker ${entry._1.hostPort}")
      } else {
        logError(s"Failed to reserve buffers from worker ${entry._1.hostPort}")
        failed.add(entry._1)
      }
    })

    failed
  }

  def reserveBuffersWithRetry(
      applicationId: String, shuffleId: Int, slots: WorkerResource): util.List[WorkerInfo] = {
    // reserve buffers
    var failed = reserveBuffers(applicationId, shuffleId, slots)

    // retry once if any failed
    failed = if (failed.nonEmpty) {
      logInfo("reserve buffers failed, retry once")
      reserveBuffers(applicationId, shuffleId, slots.filterKeys(worker => failed.contains(worker)))
    } else null

    // add into blacklist
    if (failed != null) {
      failed.foreach(w => blacklist.add(w.hostPort))
    }

    failed
  }

  def destroyBuffersWithRetry(shuffleKey: String, worker: WorkerInfo,
    masterLocations: util.List[String], slaveLocations: util.List[String]
  ): (util.List[String], util.List[String]) = {
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
    case RegisterWorker(host, port, numSlots, worker) =>
      logInfo(s"received RegisterWorker request, $host:$port $numSlots")
      handleRegisterWorker(context, host, port, numSlots, worker)

    case RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions) =>
      logDebug(s"received RegisterShuffle request, " +
        s"$applicationId, $shuffleId, $numMappers, $numPartitions")
      handleRegisterShuffle(context, applicationId, shuffleId, numMappers, numPartitions)

    case Revive(applicationId, shuffleId, oldPartition) =>
      logDebug(s"received Revive request, $applicationId, $shuffleId, $oldPartition")
      handleRevive(context, applicationId, shuffleId, oldPartition)

    case MapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers) =>
      logDebug(s"received MapperEnd request, $applicationId, $shuffleId, $mapId, $attemptId")
      handleMapperEnd(context, applicationId, shuffleId, mapId, attemptId, numMappers)

    case GetReducerFileGroup(applicationId: String, shuffleId: Int) =>
      logDebug(s"received GetShuffleFileGroup request, $applicationId, $shuffleId")
      handleGetReducerFileGroup(context, applicationId, shuffleId)

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
      if (workers.get(ind).lastHeartbeat < currentTime - WORKER_TIMEOUT_MS
        && !workerLostEvents.contains(workers.get(ind).hostPort)) {
        logInfo(s"Worker ${workers.get(ind)} timeout! Trigger WorkerLost event")
        // trigger WorkerLost event
        workerLostEvents.add(workers.get(ind).hostPort)
        self.send(
          WorkerLost(workers.get(ind).host, workers.get(ind).port)
        )
      }
      ind += 1
    }
  }

  private def timeoutDeadApplications(): Unit = {
    logInfo("timeoutDeadApplications")
    val currentTime = System.currentTimeMillis()
    val keys = appHeartbeatTime.keySet()
    keys.foreach(key => {
      if (appHeartbeatTime.get(key) < currentTime - APPLICATION_TIMEOUT_MS) {
        logError(s"Application $key timeout! Trigger ApplicationLost event")
        var res = self.askSync[ApplicationLostResponse](
          ApplicationLost(key)
        )
        var retry = 1
        while (res.status != StatusCode.Success && retry <= 3) {
          res = self.askSync[ApplicationLostResponse](
            ApplicationLost(key)
          )
          retry += 1
        }
        if (retry > 3) {
          logError("HandleApplicationLost failed more than 3 times!")
        }
        appHeartbeatTime.remove(key)
      }
    })
  }

  private def handleHeartBeatFromWorker(host: String, port: Int): Unit = {
    logDebug(s"received heartbeat from $host:$port")
    val worker: WorkerInfo = workers.find(w => w.host == host && w.port == port).orNull
    if (worker == null) {
      logInfo(s"received heartbeat from unknown worker! $host:$port")
      return
    }
    worker.synchronized {
      worker.lastHeartbeat = System.currentTimeMillis()
    }
    blacklist.remove(host + ":" + port)
  }

  private def handleWorkerLost(context: RpcCallContext, host: String, port: Int): Unit = {
    // find worker
    val worker: WorkerInfo = workers.find(w => w.host == host && w.port == port).orNull
    if (worker == null) {
      logError(s"Unknown worker $host:$port for WorkerLost handler!")
      return
    }
    // remove worker from workers
    workers.synchronized {
      workers.remove(worker)
    }
    // delete from blacklist
    blacklist.remove(worker.hostPort)

    // for all master/slave partitions on the lost worker, send CommitFiles to their peer patitions
    // then destroy the partitions
    def commitAndDestroy(mode: PartitionLocation.Mode): Unit = {
      val shuffleKeys = worker.getMasterShuffleKeys()
      shuffleKeys.foreach(shuffleKey => {
        // all peer locations for the shuffle key
        val (locations, flippedMode) =
          if (mode == PartitionLocation.Mode.Master) {
            (worker.getAllMasterLocations(shuffleKey).map(_.getPeer),
              PartitionLocation.Mode.Slave)
          } else {
            (worker.getAllSlaveLocations(shuffleKey).map(_.getPeer),
              PartitionLocation.Mode.Master)
          }
        val groupedByWorker = locations.groupBy(loc => loc.hostPort())

        groupedByWorker.foreach(elem => {
          val worker = workers.find(w => w.hostPort == elem._1).orNull
          // commit files
          logDebug(s"send commit file request to ${worker.hostPort}, " +
            s"${elem._2.map(_.getUniqueId).mkString(",")}")
          val res = worker.endpoint.askSync[CommitFilesResponse](
            CommitFiles(shuffleKey, elem._2.map(_.getUniqueId()).toList, flippedMode)
          )
          // record committed Files
          val committedPartitions = shuffleCommittedPartitions.get(shuffleKey)
          committedPartitions.synchronized {
            val committed = elem._2.filter(p => res.committedLocations.contains(p.getUniqueId))
            committedPartitions.addAll(committed)
            if (mode == PartitionLocation.Mode.Master) {
              committedPartitions.addAll(committed.map(_.getPeer))
            }
          }
          // destroy peer partitions
          val destroyMsg =
            if (mode == PartitionLocation.Mode.Master) {
              Destroy(shuffleKey, null, elem._2.map(_.getUniqueId).toList)
            } else {
              Destroy(shuffleKey, elem._2.map(_.getUniqueId).toList, null)
            }
          val resDestroy = worker.endpoint.askSync[DestroyResponse](
            destroyMsg
          )
          // retry once to destroy
          if (resDestroy.status != StatusCode.Success) {
            logError("destroy failed when handling WorkerLost!")
          }
          // remove partitions
          if (mode == PartitionLocation.Mode.Master) {
            worker.removeSlavePartitions(shuffleKey, elem._2.map(_.getUniqueId))
          } else {
            worker.removeMasterPartitions(shuffleKey, elem._2.map(_.getUniqueId))
          }
        })
      })
    }

    commitAndDestroy(PartitionLocation.Mode.Master)
    commitAndDestroy(PartitionLocation.Mode.Slave)

    logInfo("Finished to process WorkerLost!")
    workerLostEvents.remove(worker.hostPort)
    if (context != null) {
      context.reply(WorkerLostResponse(true))
    }
  }

  def handleMasterPartitionSuicide(context: RpcCallContext,
    shuffleKey: String, location: PartitionLocation): Unit = {
    val worker: WorkerInfo = workers.find(w => w.hostPort == location.hostPort()).orNull
    if (worker == null) {
      logError(s"worker not found for the location $location !")
      context.reply(MasterPartitionSuicideResponse(StatusCode.WorkerNotFound))
      return
    }

    worker.removeMasterPartition(shuffleKey, location.getUniqueId)
    context.reply(MasterPartitionSuicideResponse(StatusCode.Success))
  }

  def handleRegisterWorker(
      context: RpcCallContext,
      host: String,
      port: Int,
      numSlots: Int,
      workerRef: RpcEndpointRef): Unit = {
    logInfo(s"Registering worker $host:$port with $numSlots slots")
    if (workers.exists(w => w.host == host && w.port == port)) {
      logError(s"Worker already registered! $host:$port")
      context.reply(RegisterWorkerResponse(false, "Worker already registered!"))
    } else {
      logInfo(s"worker info numSlots $numSlots")
      val worker = new WorkerInfo(host, port, numSlots, workerRef)
      workers.synchronized {
        workers.add(worker)
      }
      logInfo(s"registered worker $worker")
      context.reply(RegisterWorkerResponse(true, null))
    }
  }

  def handleRegisterShuffle(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)

    // check if same request already exists for the same shuffle.
    // If do, just register and return
    registerShuffleRequest.synchronized {
      if (registerShuffleRequest.containsKey(shuffleKey)) {
        logDebug("[handleRegisterShuffle] request for same shuffleKey exists, just register")
        registerShuffleRequest.get(shuffleKey).add(context)
        return
      } else {
        // check if shuffle is registered
        if (registeredShuffle.contains(shuffleKey)) {
          val locs = workers.flatMap(w => w.getAllMasterLocationsWithMaxEpoch(shuffleKey))
          logDebug(s"shuffle $shuffleKey already registered, just return")
          if (locs.size != numPartitions) {
            logError(s"location size not equal to numPartitions: $numPartitions!")
          }
          context.reply(RegisterShuffleResponse(StatusCode.Success, locs))
          return
        }
        logInfo(s"new shuffle request, shuffleKey $shuffleKey")
        val set = new util.HashSet[RpcCallContext]()
        set.add(context)
        registerShuffleRequest.put(shuffleKey, set)
      }
    }

    // offer slots
    val slots = workers.synchronized {
      MasterUtil.offerSlots(
        shuffleKey,
        workersNotBlacklisted(),
        (0 until numPartitions).map(new Integer(_)).toList
      )
    }

    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError(s"offerSlots failed $shuffleKey!")
      context.reply(RegisterShuffleResponse(StatusCode.SlotNotAvailable, null))
      return
    }

    // reserve buffers
    val failed = reserveBuffersWithRetry(applicationId, shuffleId, slots)

    // reserve buffers failed, clear allocated resources
    if (failed != null && !failed.isEmpty()) {
      logError("reserve buffers still fail after retry, clear buffers")
      slots.foreach(entry => {
        destroyBuffersWithRetry(shuffleKey, entry._1,
          entry._2._1.map(_.getUniqueId),
          entry._2._2.map(_.getUniqueId))
        entry._1.removeMasterPartitions(shuffleKey, entry._2._1.map(_.getUniqueId))
        entry._1.removeSlavePartitions(shuffleKey, entry._2._2.map(_.getUniqueId))
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
      if (!shuffleMapperAttempts.containsKey(shuffleKey)) {
        val attempts = new Array[Int](numMappers)
        0 until numMappers foreach (idx => attempts(idx) = -1)
        shuffleMapperAttempts.synchronized {
          shuffleMapperAttempts.put(shuffleKey, attempts)
        }
      }
    }
    shuffleCommittedPartitions.synchronized {
      val committedPartitions = new ConcurrentSet[PartitionLocation]()
      shuffleCommittedPartitions.put(shuffleKey, committedPartitions)
    }

    reducerFileGroupsMap.put(shuffleKey, new Array[Array[PartitionLocation]](numPartitions))

    logInfo(s"Handle RegisterShuffle Success, $shuffleKey")
    registerShuffleRequest.synchronized {
      val set = registerShuffleRequest.get(shuffleKey)
      set.foreach(context => {
        context.reply(RegisterShuffleResponse(StatusCode.Success, locations))
      })
      registerShuffleRequest.remove(shuffleKey)
    }
  }

  private def handleRevive(context: RpcCallContext,
    applicationId: String,
    shuffleId: Int,
    oldPartition: PartitionLocation): Unit = {
    // check whether shuffle has registered
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    if (!registeredShuffle.contains(shuffleKey)) {
      logError(s"shuffle $shuffleKey not registered!")
      context.reply(ReviveResponse(StatusCode.ShuffleNotRegistered, null))
      return
    }

    def checkAndBlacklist(loc: PartitionLocation): Unit = {
      val worker = workers.find(w => w.hostPort == loc.hostPort()).orNull
      if (worker == null || !worker.endpoint.asInstanceOf[NettyRpcEndpointRef].client.isActive) {
        blacklist.add(loc.hostPort())
      }
    }

    // check to see if partition can be reached. if not, add into blacklist
    checkAndBlacklist(oldPartition)
    checkAndBlacklist(oldPartition.getPeer)

    // check if there exists request for the partition, if do just register
    reviving.synchronized {
      reviving.putIfAbsent(shuffleKey,
        new util.HashMap[PartitionLocation, util.Set[RpcCallContext]]())
    }
    val shuffleReviving = reviving.get(shuffleKey)
    shuffleReviving.synchronized {
      if (shuffleReviving.containsKey(oldPartition)) {
        shuffleReviving.get(oldPartition).add(context)
        logInfo("same partition is reviving, register context")
        return
      } else {
        // check if new slot for the partition has allocated
        val locs = workers.flatMap(worker => {
          worker.getLocationWithMaxEpoch(shuffleKey, oldPartition.getReduceId)
        })
        var currentEpoch = -1
        var currentLocation: PartitionLocation = null
        locs.foreach(loc => {
          if (loc.getEpoch > currentEpoch) {
            currentEpoch = loc.getEpoch
            currentLocation = loc
          }
        })
        // exists newer partition, just return it
        if (currentEpoch > oldPartition.getEpoch) {
          context.reply(ReviveResponse(StatusCode.Success, currentLocation))
          logDebug(s"new partition found, return it ${shuffleKey} " + currentLocation)
          return
        }
        // no newer partition, register and allocate
        val set = new util.HashSet[RpcCallContext]()
        set.add(context)
        shuffleReviving.put(oldPartition, set)
      }
    }

    // offer new slot
    val slots = workers.synchronized {
      val availableWorkers = workersNotBlacklisted()
      MasterUtil.offerSlots(shuffleKey,
        // avoid offer slots on the same host of oldPartition
        workersNotBlacklisted(),
        Seq(new Integer(oldPartition.getReduceId)),
        Array(oldPartition.getEpoch)
      )
    }
    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError("offerSlot failed!")
      shuffleReviving.synchronized {
        val set = shuffleReviving.get(oldPartition)
        set.foreach(_.reply(ReviveResponse(StatusCode.SlotNotAvailable, null)))
      }
      return
    }
    logInfo("offered slots success")
    // reserve buffer
    val failed = reserveBuffersWithRetry(applicationId, shuffleId, slots)
    // reserve buffers failed, clear allocated resources
    if (failed != null && !failed.isEmpty()) {
      slots.foreach(entry => {
        destroyBuffersWithRetry(shuffleKey,
          entry._1, entry._2._1.map(_.getUniqueId),
          entry._2._2.map(_.getUniqueId))
        entry._1.removeMasterPartitions(shuffleKey, entry._2._1.map(_.getUniqueId))
        entry._1.removeSlavePartitions(shuffleKey, entry._2._2.map(_.getUniqueId))
      })
      logError("fail to reserve buffers")
      shuffleReviving.synchronized {
        val set = shuffleReviving.get(oldPartition)
        set.foreach(_.reply(ReviveResponse(StatusCode.ReserveBufferFailed, null)))
      }
      return
    }

    // reply success
    val (masters, slaves) = slots.head._2
    val location = if (masters != null && masters.size() > 0) {
      masters.head
    } else {
      slaves.head.getPeer
    }
    logDebug(s"reserve buffer success ${shuffleKey} " + location)
    shuffleReviving.synchronized {
      val set = shuffleReviving.get(oldPartition)
      set.foreach(_.reply(ReviveResponse(StatusCode.Success, location)))
      shuffleReviving.remove(oldPartition)
      logInfo("reply and remove oldPartition success")
    }
  }

  private def handleMapperEnd(context: RpcCallContext,
    applicationId: String,
    shuffleId: Int,
    mapId: Int,
    attemptId: Int,
    numMappers: Int): Unit = {
    var askStageEnd: Boolean = false
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    // update max attemptId
    shuffleMapperAttempts.synchronized {
      var attempts = shuffleMapperAttempts.get(shuffleKey)
      if (attempts == null) {
        logInfo(s"[handleMapperEnd] shuffle $shuffleKey not registered, create one")
        attempts = new Array[Int](numMappers)
        0 until numMappers foreach (ind => attempts(ind) = -1)
        shuffleMapperAttempts.put(shuffleKey, attempts)
      }

      if (attempts(mapId) < 0) {
        attempts(mapId) = attemptId
      } else {
        // Mapper with another attemptId called, skip this request
        context.reply(MapperEndResponse(StatusCode.Success))
        return
      }

      if (!attempts.exists(_ < 0)) {
        askStageEnd = true
      }
    }

    if (askStageEnd) {
      // last mapper finished. call mapper end
      logInfo(s"Last MapperEnd, call StageEnd with shuffleKey: $applicationId-$shuffleId")
      self.send(StageEnd(applicationId, shuffleId))
    }

    // reply success
    context.reply(MapperEndResponse(StatusCode.Success))
  }

  private def handleGetReducerFileGroup(context: RpcCallContext,
    applicationId: String,
    shuffleId: Int): Unit = {

    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)

    var timeout = EssConf.essStageEndTimeout(conf)
    val delta = 50
    while (!stageEndShuffleSet.contains(shuffleKey)) {
      logInfo(s"wait for StageEnd, ${shuffleKey}")
      Thread.sleep(50)
      if (timeout <= 0) {
        logError(s"StageEnd Timeout! ${shuffleKey}")
        context.reply(GetReducerFileGroupResponse(StatusCode.Failed, null, null))
        return
      }
      timeout = timeout - delta
    }

    val shuffleFileGroup = reducerFileGroupsMap.get(shuffleKey)

    context.reply(GetReducerFileGroupResponse(
      StatusCode.Success,
      shuffleFileGroup,
      shuffleMapperAttempts.get(shuffleKey)
    ))
  }

  @Deprecated
  private def handleSlaveLost(context: RpcCallContext,
    shuffleKey: String, masterLocation: PartitionLocation,
    slaveLocation: PartitionLocation): Unit = {
    // find slaveWorker
    val slaveWorker = workers.find(w => w.host == slaveLocation.getHost &&
      w.port == slaveLocation.getPort).orNull
    val masterWorker = workers.find(w => w.host == masterLocation.getHost &&
      w.port == masterLocation.getPort).orNull
    // send Destroy
    if (slaveWorker != null) {
      val (_, failedSlave) = destroyBuffersWithRetry(shuffleKey,
        slaveWorker, null, List(slaveLocation.getUniqueId))
      if (failedSlave == null || failedSlave.isEmpty) {
        // remove slave partition
        if (slaveWorker != null) {
          slaveWorker.removeSlavePartition(shuffleKey, slaveLocation.getUniqueId)
        }
      }
    }
    // update master locations's peer
    masterWorker.setMasterPeer(shuffleKey, masterLocation, null)
    // offer new slot
    val location = workers.synchronized {
      MasterUtil.offerSlaveSlot(slaveLocation.getPeer, workersNotBlacklisted())
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
    val failed = "" //reserveBuffersWithRetry(shuffleKey, slots)
    if (failed != null && !failed.isEmpty()) {
      logError("reserve buffer failed!")
      // update status
      if (slaveWorker != null) {
        slaveWorker.removeSlavePartition(shuffleKey, location._2.getUniqueId)
      }
      context.reply(SlaveLostResponse(StatusCode.ReserveBufferFailed, null))
      return
    }
    // add slave partition
    location._1.addSlavePartition(shuffleKey, location._2)
    // update peer
    masterWorker.setMasterPeer(shuffleKey, masterLocation, location._2)
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
      logInfo(s"[handleStageEnd] shuffle $shuffleKey not registered, maybe no shuffle data")
      // record in stageEndShuffleSet
      stageEndShuffleSet.synchronized {
        stageEndShuffleSet.add(shuffleKey)
      }
      if (context != null) {
        context.reply(StageEndResponse(StatusCode.ShuffleNotRegistered, null))
      }
      return
    }

    // ask allLocations workers holding master partitions to commit files
    val failedMasters: util.List[PartitionLocation] = new util.ArrayList[PartitionLocation]()
    val failedSlaves: util.List[PartitionLocation] = new util.ArrayList[PartitionLocation]()

    // check connection before commit files
    workers.foreach(w => {
      if (!w.endpoint.asInstanceOf[NettyRpcEndpointRef].client.isActive
        && !workerLostEvents.contains(w.hostPort)) {
        logInfo(s"Find WorkerLost in StageEnd ${w.hostPort}")
        self.send(WorkerLost(w.host, w.port))
        workerLostEvents.add(w.hostPort)
      }
    })

    // wait until all workerLost events are handled
    while (!workerLostEvents.isEmpty) {
      Thread.sleep(50)
      logWarning("wait for WorkerLost events all handled")
    }

    ThreadUtils.parmap(
      workers.to, "CommitFiles Master", EssConf.essRpcParallelism(conf)) { worker =>
      if (worker.containsShuffleMaster(shuffleKey)) {
        val masterLocs = worker.getAllMasterLocations(shuffleKey)
        val start = System.currentTimeMillis()
        val res = worker.endpoint.askSync[CommitFilesResponse](
          CommitFiles(shuffleKey,
            masterLocs.map(_.getUniqueId),
            PartitionLocation.Mode.Master)
        )
        // record failed masters
        if (res.failedLocations != null) {
          res.failedLocations.foreach(id => {
            failedMasters.add(masterLocs.find(loc => loc.getUniqueId == id).get)
          })
        }
        // record committed partitionIds
        val committedPartitions = shuffleCommittedPartitions.get(shuffleKey)
        committedPartitions.synchronized {
          res.committedLocations.foreach(id => {
            committedPartitions.add(masterLocs.find(loc => loc.getUniqueId == id).get)
          })
        }
        logDebug(s"Finished CommitFiles Master Mode for worker ${
          worker.endpoint.address
            .toEssURL
        } in ${System.currentTimeMillis() - start}ms")
      }
    }

    // if any failedMasters, ask allLocations workers holding slave partitions to commit files
    if (!failedMasters.isEmpty) {
      // group allLocations failedMasters partitions by location
      val grouped = failedMasters.map(_.getPeer).groupBy(loc => loc.hostPort())
      ThreadUtils.parmap(
        grouped.to, "", EssConf.essRpcParallelism(conf)
      ) {
        entry =>
          val start = System.currentTimeMillis()
          val worker = workers.find(w => w.hostPort.equals(entry._1)).get
          val slavePartitions = entry._2.toList
          val res = worker.endpoint.askSync[CommitFilesResponse](
            CommitFiles(shuffleKey,
              slavePartitions.map(_.getUniqueId),
              PartitionLocation.Mode.Master)
          )
          // record failed locations
          if (res.failedLocations != null) {
            failedSlaves.addAll(
              slavePartitions.filter(loc => res.failedLocations.contains(loc.getUniqueId)))
          }
          // record committed partition ids
          val committedPartitions = shuffleCommittedPartitions.get(shuffleKey)
          committedPartitions.synchronized {
            committedPartitions.addAll(
              slavePartitions.filter(loc => res.committedLocations.contains(loc.getUniqueId)))
          }
          logDebug(s"Finished CommitFiles Slave Mode for worker ${
            worker.endpoint.address
              .toEssURL
          } in ${System.currentTimeMillis() - start}ms")
      }
    }

    // ask all workers holding master/slave partition to release resource
    workers.foreach(worker => {
      val allMasterIds = worker.getAllMasterIds(shuffleKey)
      val allSlaveIds = worker.getAllSlaveIds(shuffleKey)
      var res = worker.endpoint.askSync[DestroyResponse](
        Destroy(shuffleKey, allMasterIds, allSlaveIds
        )
      )
      // retry once to destroy
      if (res.status != StatusCode.Success) {
        res = worker.endpoint.askSync[DestroyResponse](
          Destroy(shuffleKey, res.failedMasters, res.failedSlaves
          )
        )
      }
    })
    // release resources and clear worker info
    workers.synchronized {
      workers.foreach { worker =>
        worker.removeMasterPartitions(shuffleKey)
        worker.removeSlavePartitions(shuffleKey)
      }
    }

    // check if committed files contains all files mappers written
    val committedPartitions = shuffleCommittedPartitions.get(shuffleKey)
    val fileGroups = reducerFileGroupsMap.get(shuffleKey)
    val sets = Array.fill(fileGroups.length)(new util.HashSet[PartitionLocation]())
    committedPartitions.foreach { partition =>
      sets(partition.getReduceId).add(partition)
    }
    var i = 0
    while (i < fileGroups.length) {
      fileGroups(i) = sets(i).toArray(new Array[PartitionLocation](0))
      i += 1
    }

    // clear committed files
    shuffleCommittedPartitions.synchronized {
      shuffleCommittedPartitions.remove(shuffleKey)
    }

    // reply
    if (failedSlaves.isEmpty) {
      // record in stageEndShuffleSet
      stageEndShuffleSet.synchronized {
        stageEndShuffleSet.add(shuffleKey)
      }
      logInfo(s"succeed to handle stageend! $shuffleKey")
      if (context != null) {
        context.reply(StageEndResponse(StatusCode.Success, null))
      }
    } else {
      logError(s"failed to handle stageend, lost file! $shuffleKey")
      if (context != null) {
        context.reply(StageEndResponse(StatusCode.PartialSuccess, null))
      }
    }
  }

  def handleUnregisterShuffle(context: RpcCallContext, appId: String, shuffleId: Int): Unit = {
    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)

    // if StageEnd has not been handled, trigger StageEnd
    if (!stageEndShuffleSet.contains(shuffleKey)) {
      logInfo(s"Call StageEnd before Unregister Shuffle $shuffleKey")
      self.send(StageEnd(appId, shuffleId))
    }

    // if PartitionLocations exists for the shuffle, return fail
    if (partitionExists(shuffleKey)) {
      logError(s"Partition exists for shuffle $shuffleKey!")
      context.reply(UnregisterShuffleResponse(StatusCode.PartitionExists))
      return
    }
    // clear shuffle attempts for the shuffle
    shuffleMapperAttempts.synchronized {
      logInfo(s"Remove from shuffleMapperAttempts, $shuffleKey")
      shuffleMapperAttempts.remove(shuffleKey)
    }
    // clear reducerFileGroup for the shuffle
    reducerFileGroupsMap.remove(shuffleKey)
    // delete shuffle files
//    val shuffleDir = EssPathUtil.GetShuffleDir(conf, appId, shuffleId)
    //val success = fs.delete(shuffleDir, true)
    if (true) {
      context.reply(UnregisterShuffleResponse(StatusCode.Success))
    } else {
//      logError(s"delete files failed! $shuffleDir")
      context.reply(UnregisterShuffleResponse(StatusCode.DeleteFilesFailed))
    }
    logInfo("unregister success")
    context.reply(UnregisterShuffleResponse(StatusCode.Success))
  }

  def handleApplicationLost(context: RpcCallContext, appId: String): Unit = {
    def getNextShufflePartitions(worker: WorkerInfo):
    (String, util.List[String], util.List[String]) = {
      val shuffleKeys = new util.HashSet[String]()
      shuffleKeys.addAll(worker.getMasterShuffleKeys())
      shuffleKeys.addAll(worker.getSlaveShuffleKeys())
      val nextShuffleKey = shuffleKeys.find(_.startsWith(appId)).orNull
      if (nextShuffleKey == null) {
        return null
      }

      val allMasterIds = worker.getAllMasterIds(nextShuffleKey)
      val allSlaveIds = worker.getAllSlaveIds(nextShuffleKey)
      (nextShuffleKey, allMasterIds, allSlaveIds)
    }

    // destroy partition buffers in workers, then update info in master
    workers.foreach(worker => {
      var nextShufflePartitions = getNextShufflePartitions(worker)
      while (nextShufflePartitions != null) {
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
          worker.removeMasterPartitions(shuffleKey, masterLocs)
          worker.removeSlavePartitions(shuffleKey, slaveLocs)
        }
        nextShufflePartitions = getNextShufflePartitions(worker)
      }
    })
    // clear reducerFileGroup for the application
    val keys = reducerFileGroupsMap.keySet()
    keys.filter(key => key.startsWith(appId)).foreach(key => reducerFileGroupsMap.remove(key))
    // delete files for the application
//    val appPath = EssPathUtil.GetAppDir(conf, appId)
//    fs.delete(appPath, true)
    logInfo("Finished handling ApplicationLost")
    context.reply(ApplicationLostResponse(StatusCode.Success))
  }

  private def handleHeartBeatFromApplication(appId: String): Unit = {
    appHeartbeatTime.synchronized {
      logInfo("heartbeat from application " + appId)
      appHeartbeatTime.put(appId, System.currentTimeMillis())
    }
  }

  private def partitionExists(shuffleKey: String): Boolean = {
    workers.exists(w => {
      w.containsShuffleMaster(shuffleKey) ||
        w.containsShuffleSlave(shuffleKey)
    })
  }

  private def workersNotBlacklisted(): util.List[WorkerInfo] = {
    workers.filter(w => !blacklist.contains(w.hostPort))
  }

  def getWorkerInfos(): String = {
    val sb = new StringBuilder
    workers.foreach(w => {
      sb.append("==========WorkerInfos in Master==========\n")
      sb.append(w).append("\n")

      val workerInfo = w.endpoint.askSync[GetWorkerInfosResponse](GetWorkerInfos)
        .workerInfos.asInstanceOf[util.List[WorkerInfo]](0)

      sb.append("==========WorkerInfos in Workers==========\n")
      sb.append(workerInfo).append("\n")

      if (w.hasSameInfoWith(workerInfo)) {
        sb.append("Consist!").append("\n")
      } else {
        sb.append("[ERROR] Inconsist!").append("\n")
      }
    })

    workers.foreach(w => {
    })

    sb.toString()
  }

  def getThreadDump(): String = {
    val sb = new StringBuilder
    val threadDump = Utils.getThreadDump()
    sb.append("==========Master ThreadDump==========\n")
    sb.append(threadDump).append("\n")
    workers.foreach(w => {
      sb.append(s"==========Worker ${w.hostPort} ThreadDump==========\n")
      val res = w.endpoint.askSync[ThreadDumpResponse](ThreadDump)
      sb.append(res.threadDump).append("\n")
    })

    sb.toString()
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
