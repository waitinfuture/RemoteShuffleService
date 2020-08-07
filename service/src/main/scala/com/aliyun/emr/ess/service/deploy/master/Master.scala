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
  private val workers = new util.ArrayList[WorkerInfo]()
  private def workersSnapShot = workers.synchronized(new util.ArrayList[WorkerInfo](workers))

  private val appHeartbeatTime = new util.HashMap[String, Long]()

  // key: "appId_shuffleId"
  private val registeredShuffle = new ConcurrentSet[String]()
  private val shuffleMapperAttempts = new ConcurrentHashMap[String, Array[Int]]()
  private val reducerFileGroupsMap =
    new ConcurrentHashMap[String, Array[Array[PartitionLocation]]]()
  private val dataLostShuffleSet = new ConcurrentSet[String]()
  private val stageEndShuffleSet = new ConcurrentSet[String]()
  private val shuffleAllocatedWorkers = new ConcurrentHashMap[String, ConcurrentSet[WorkerInfo]]()

  // revive request waiting for response
  // shuffleKey -> (partitionId -> set)
  private val reviving =
    new util.HashMap[String, util.HashMap[String, util.Set[RpcCallContext]]]()

  // register shuffle request waiting for response
  private val registerShuffleRequest = new ConcurrentHashMap[String, util.Set[RpcCallContext]]()

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

    slots.foreach { entry =>
      val res = requestReserveBuffers(entry._1.endpoint,
        ReserveBuffers(applicationId, shuffleId, entry._2._1, entry._2._2))
      if (res.status.equals(StatusCode.Success)) {
        logInfo(s"Successfully allocated partitions buffer from worker ${entry._1.hostPort}")
      } else {
        logError(s"Failed to reserve buffers from worker ${entry._1.hostPort}")
        failed.add(entry._1)
      }
    }

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

  def destroyBuffersWithRetry(
      shuffleKey: String,
      worker: WorkerInfo,
      masterLocations: util.List[String],
      slaveLocations: util.List[String]): (util.List[String], util.List[String]) = {
    var res = requestDestroy(worker.endpoint, Destroy(shuffleKey, masterLocations, slaveLocations))
    if (res.status != StatusCode.Success) {
      res = requestDestroy(worker.endpoint,
        Destroy(shuffleKey, res.failedMasters, res.failedSlaves))
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

    case Revive(applicationId, shuffleId, reduceId, epoch, oldPartition) =>
      logDebug(s"received Revive request, $applicationId, $shuffleId, " +
        s"$reduceId-$epoch $oldPartition")
      handleRevive(context, applicationId, shuffleId, reduceId, epoch, oldPartition)

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

    case HeartbeatFromWorker(host, port, shuffleKeys) =>
      handleHeartBeatFromWorker(context, host, port, shuffleKeys)
  }

  private def timeoutDeadWorkers() {
    val currentTime = System.currentTimeMillis()
    var ind = 0
    workersSnapShot.foreach { worker =>
      if (worker.lastHeartbeat < currentTime - WORKER_TIMEOUT_MS
        && !workerLostEvents.contains(worker.hostPort)) {
        logInfo(s"Worker ${worker} timeout! Trigger WorkerLost event")
        // trigger WorkerLost event
        workerLostEvents.add(worker.hostPort)
        self.send(WorkerLost(worker.host, worker.port))
      }
      ind += 1
    }
  }

  private def timeoutDeadApplications(): Unit = {
    logInfo("timeoutDeadApplications")
    val currentTime = System.currentTimeMillis()
    val keys = appHeartbeatTime.keySet().toList
    keys.foreach { key =>
      if (appHeartbeatTime.get(key) < currentTime - APPLICATION_TIMEOUT_MS) {
        logError(s"Application $key timeout! Trigger ApplicationLost event")
        var res = self.askSync[ApplicationLostResponse](ApplicationLost(key))
        var retry = 1
        while (res.status != StatusCode.Success && retry <= 3) {
          res = self.askSync[ApplicationLostResponse](ApplicationLost(key))
          retry += 1
        }
        if (retry > 3) {
          logError("HandleApplicationLost failed more than 3 times!")
        }
        appHeartbeatTime.remove(key)
      }
    }
  }

  private def handleHeartBeatFromWorker(
      context: RpcCallContext,
      host: String,
      port: Int,
      shuffleKeys: util.HashSet[String]): Unit = {
    logDebug(s"received heartbeat from $host:$port")
    val worker: WorkerInfo = workersSnapShot.find(w => w.host == host && w.port == port).orNull
    if (worker == null) {
      logInfo(s"received heartbeat from unknown worker! $host:$port")
      return
    }
    worker.synchronized {
      worker.lastHeartbeat = System.currentTimeMillis()
    }
    blacklist.remove(host + ":" + port)

    val expiredShuffleKeys = new util.HashSet[String]
    shuffleKeys.foreach { shuffleKey =>
      if (!registerShuffleRequest.containsKey(shuffleKey) &&
        !registeredShuffle.contains(shuffleKey)) {
        logWarning(s"shuffle $shuffleKey expired on $host:$port")
        expiredShuffleKeys.add(shuffleKey)
      }
    }
    context.reply(HeartbeatResponse(expiredShuffleKeys))
  }

  private def handleWorkerLost(context: RpcCallContext, host: String, port: Int): Unit = {
    // find worker
    val worker: WorkerInfo = workersSnapShot.find(w => w.host == host && w.port == port).orNull
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

    logInfo("Finished to process WorkerLost!")
    workerLostEvents.remove(worker.hostPort)
    if (context != null) {
      context.reply(WorkerLostResponse(true))
    }
  }

  def handleMasterPartitionSuicide(
      context: RpcCallContext,
      shuffleKey: String,
      location: PartitionLocation): Unit = {
    val worker: WorkerInfo = workersSnapShot.find(w => w.hostPort == location.hostPort()).orNull
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
    if (workersSnapShot.exists(w => w.host == host && w.port == port)) {
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
          val locs = workersSnapShot.flatMap(w => w.getAllMasterLocationsWithMaxEpoch(shuffleKey))
          logDebug(s"shuffle $shuffleKey already registered, just return")
          if (locs.size != numPartitions) {
            logWarning(s"shuffle $shuffleKey location size ${locs.size} not equal to " +
              s"numPartitions: $numPartitions!")
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
      registerShuffleRequest.synchronized {
        val set = registerShuffleRequest.get(shuffleKey)
        set.foreach { context =>
          context.reply(RegisterShuffleResponse(StatusCode.SlotNotAvailable, null))
        }
        registerShuffleRequest.remove(shuffleKey)
      }
      return
    }

    // reserve buffers
    val failed = reserveBuffersWithRetry(applicationId, shuffleId, slots)

    // reserve buffers failed, clear allocated resources
    if (failed != null && !failed.isEmpty()) {
      logWarning("reserve buffers still fail after retry, clear buffers")
      slots.foreach(entry => {
        destroyBuffersWithRetry(shuffleKey, entry._1,
          entry._2._1.map(_.getUniqueId),
          entry._2._2.map(_.getUniqueId))
        entry._1.removeMasterPartitions(shuffleKey, entry._2._1.map(_.getUniqueId))
        entry._1.removeSlavePartitions(shuffleKey, entry._2._2.map(_.getUniqueId))
      })
      logError(s"registerShuffle $shuffleKey failed to reserve buffers, reply to all")
      registerShuffleRequest.synchronized {
        val set = registerShuffleRequest.get(shuffleKey)
        set.foreach { context =>
          context.reply(RegisterShuffleResponse(StatusCode.ReserveBufferFailed, null))
        }
        registerShuffleRequest.remove(shuffleKey)
      }
      return
    }

    val allocatedWorkers = new ConcurrentSet[WorkerInfo]
    allocatedWorkers.addAll(slots.keySet())
    shuffleAllocatedWorkers.put(shuffleKey, allocatedWorkers)

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

    reducerFileGroupsMap.put(shuffleKey, new Array[Array[PartitionLocation]](numPartitions))

    logInfo(s"Handle RegisterShuffle Success, $shuffleKey")
    registerShuffleRequest.synchronized {
      val set = registerShuffleRequest.get(shuffleKey)
      set.foreach { context =>
        context.reply(RegisterShuffleResponse(StatusCode.Success, locations))
      }
      registerShuffleRequest.remove(shuffleKey)
    }
  }

  private def handleRevive(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      reduceId: Int,
      oldEpoch: Int,
      oldPartition: PartitionLocation): Unit = {
    // check whether shuffle has registered
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    if (!registeredShuffle.contains(shuffleKey)) {
      logError(s"shuffle $shuffleKey not registered!")
      context.reply(ReviveResponse(StatusCode.ShuffleNotRegistered, null))
      return
    }

    def checkAndBlacklist(loc: PartitionLocation): Unit = {
      val worker = workersSnapShot.find(w => w.hostPort == loc.hostPort()).orNull
      if (worker != null && !worker.endpoint.asInstanceOf[NettyRpcEndpointRef].client.isActive) {
        logWarning(s"add ${worker.hostPort} to blacklist")
        blacklist.add(loc.hostPort())
      }
    }

    if (oldPartition != null) {
      // check to see if partition can be reached. if not, add into blacklist
      checkAndBlacklist(oldPartition)
      checkAndBlacklist(oldPartition.getPeer)
    }

    val oldPartitionId = s"$reduceId-$oldEpoch"

    // check if there exists request for the partition, if do just register
    reviving.synchronized {
      reviving.putIfAbsent(shuffleKey, new util.HashMap[String, util.Set[RpcCallContext]])
    }
    val shuffleReviving = reviving.get(shuffleKey)
    shuffleReviving.synchronized {
      if (shuffleReviving.containsKey(oldPartitionId)) {
        shuffleReviving.get(oldPartitionId).add(context)
        logInfo(s"$shuffleKey same partition $oldPartitionId is reviving, register context")
        return
      } else {
        // check if new slot for the partition has allocated
        val locs = workersSnapShot.flatMap(_.getLocationWithMaxEpoch(shuffleKey, reduceId))
        var currentEpoch = -1
        var currentLocation: PartitionLocation = null
        locs.foreach { loc =>
          if (loc.getEpoch > currentEpoch) {
            currentEpoch = loc.getEpoch
            currentLocation = loc
          }
        }
        // exists newer partition, just return it
        if (currentEpoch > oldEpoch) {
          context.reply(ReviveResponse(StatusCode.Success, currentLocation))
          logInfo(s"new partition found, return it $shuffleKey " + currentLocation)
          return
        }
        // no newer partition, register and allocate
        val set = new util.HashSet[RpcCallContext]()
        set.add(context)
        shuffleReviving.put(oldPartitionId, set)
      }
    }

    // offer new slot
    val slots = workers.synchronized {
      MasterUtil.offerSlots(shuffleKey,
        // avoid offer slots on the same host of oldPartition
        workersNotBlacklisted(),
        Seq(new Integer(reduceId)),
        Array(oldEpoch)
      )
    }
    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError("offerSlot failed!")
      shuffleReviving.synchronized {
        val set = shuffleReviving.get(oldPartitionId)
        set.foreach(_.reply(ReviveResponse(StatusCode.SlotNotAvailable, null)))
      }
      return
    }
    logInfo("revive offered slots success")
    // reserve buffer
    val failed = reserveBuffersWithRetry(applicationId, shuffleId, slots)
    // reserve buffers failed, clear allocated resources
    if (failed != null && !failed.isEmpty()) {
      slots.foreach { entry =>
        destroyBuffersWithRetry(shuffleKey,
          entry._1, entry._2._1.map(_.getUniqueId),
          entry._2._2.map(_.getUniqueId))
        entry._1.removeMasterPartitions(shuffleKey, entry._2._1.map(_.getUniqueId))
        entry._1.removeSlavePartitions(shuffleKey, entry._2._2.map(_.getUniqueId))
      }
      logError("revive fail to reserve buffers")
      shuffleReviving.synchronized {
        val set = shuffleReviving.get(oldPartitionId)
        set.foreach(_.reply(ReviveResponse(StatusCode.ReserveBufferFailed, null)))
      }
      return
    }

    shuffleAllocatedWorkers.get(shuffleKey).addAll(slots.keySet())

    // reply success
    val (masters, slaves) = slots.head._2
    val location = if (masters != null && masters.size() > 0) {
      masters.head
    } else {
      slaves.head.getPeer
    }
    logInfo(s"revive reserve buffer success $shuffleKey $location")
    shuffleReviving.synchronized {
      val set = shuffleReviving.get(oldPartitionId)
      set.foreach(_.reply(ReviveResponse(StatusCode.Success, location)))
      shuffleReviving.remove(oldPartitionId)
      logInfo(s"reply and remove $shuffleKey $oldPartitionId partition success")
    }
  }

  private def handleMapperEnd(
      context: RpcCallContext,
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

  private def handleGetReducerFileGroup(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int): Unit = {

    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)

    var timeout = EssConf.essStageEndTimeout(conf)
    val delta = 50
    while (!stageEndShuffleSet.contains(shuffleKey)) {
      logInfo(s"wait for StageEnd, $shuffleKey")
      Thread.sleep(50)
      if (timeout <= 0) {
        logError(s"StageEnd Timeout! $shuffleKey")
        context.reply(GetReducerFileGroupResponse(StatusCode.Failed, null, null))
        return
      }
      timeout = timeout - delta
    }

    if (dataLostShuffleSet.contains(shuffleKey)) {
      context.reply(GetReducerFileGroupResponse(StatusCode.Failed, null, null))
    }

    val shuffleFileGroup = reducerFileGroupsMap.get(shuffleKey)
    context.reply(GetReducerFileGroupResponse(
      StatusCode.Success,
      shuffleFileGroup,
      shuffleMapperAttempts.get(shuffleKey)
    ))
  }

  private def handleGetWorkerInfos(context: RpcCallContext): Unit = {
    context.reply(GetWorkerInfosResponse(StatusCode.Success, workersSnapShot))
  }

  private def handleStageEnd(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int): Unit = {
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

    // check connection before commit files
    workersSnapShot.foreach { w =>
      if (!w.endpoint.asInstanceOf[NettyRpcEndpointRef].client.isActive
        && !workerLostEvents.contains(w.hostPort)) {
        logInfo(s"Find WorkerLost in StageEnd ${w.hostPort}")
        self.send(WorkerLost(w.host, w.port))
        workerLostEvents.add(w.hostPort)
      }
    }

    // wait until all workerLost events are handled
    while (!workerLostEvents.isEmpty) {
      Thread.sleep(50)
      logWarning("wait for WorkerLost events all handled")
    }

    // ask allLocations workers holding partitions to commit files
    val masterPartMap = new ConcurrentHashMap[String, PartitionLocation]
    val slavePartMap = new ConcurrentHashMap[String, PartitionLocation]
    val committedMasterIds = new ConcurrentSet[String]
    val committedSlaveIds = new ConcurrentSet[String]
    val failedMasterIds = new ConcurrentSet[String]
    val failedSlaveIds = new ConcurrentSet[String]

    val allocatedWorkers = shuffleAllocatedWorkers.get(shuffleKey)

    ThreadUtils.parmap(
      allocatedWorkers.to, "CommitFiles", EssConf.essRpcParallelism(conf)) { worker =>
      if (worker.containsShuffle(shuffleKey)) {
        val masterParts = worker.getAllMasterLocations(shuffleKey)
        val slaveParts = worker.getAllSlaveLocations(shuffleKey)
        masterParts.foreach(p => masterPartMap.put(p.getUniqueId, p))
        slaveParts.foreach(p => slavePartMap.put(p.getUniqueId, p))

        val masterIds = masterParts.map(_.getUniqueId)
        val slaveIds = slaveParts.map(_.getUniqueId)

        val start = System.currentTimeMillis()
        val res = requestCommitFiles(worker.endpoint, CommitFiles(shuffleKey, masterIds, slaveIds))

        // record committed partitionIds
        if (res.committedMasterIds != null) {
          committedMasterIds.addAll(res.committedMasterIds)
        }
        if (res.committedSlaveIds != null) {
          committedSlaveIds.addAll(res.committedSlaveIds)
        }

        // record failed partitions
        if (res.failedMasterIds != null) {
          failedMasterIds.addAll(res.failedMasterIds)
        }
        if (res.failedSlaveIds != null) {
          failedSlaveIds.addAll(res.failedSlaveIds)
        }

        logDebug(s"Finished CommitFiles for worker " +
          s"${worker.endpoint.address.toEssURL} in ${System.currentTimeMillis() - start}ms")
      }
    }

    // release resources and clear worker info
    workers.synchronized {
      workers.foreach { worker =>
        worker.removeMasterPartitions(shuffleKey)
        worker.removeSlavePartitions(shuffleKey)
      }
    }

    def hasCommonFailedIds(): Boolean = {
      for (id <- failedMasterIds) {
        if(failedSlaveIds.contains(id)) {
          logError(s"shuffle $shuffleKey partition $id: data lost")
          return true
        }
      }
      false
    }

    val dataLost = hasCommonFailedIds()

    if (!dataLost) {
      val committedPartitions = new util.HashMap[String, PartitionLocation]
      committedMasterIds.foreach { id =>
        val masterPartition = new PartitionLocation(masterPartMap.get(id))
        masterPartition.setPeer(null)
        committedPartitions.put(id, masterPartition)
      }
      committedSlaveIds.foreach { id =>
        val slavePartition = new PartitionLocation(slavePartMap.get(id))
        slavePartition.setPeer(null)
        val masterPartition = committedPartitions.get(id)
        if (masterPartition ne null) {
          masterPartition.setPeer(slavePartition)
        } else {
          logWarning(s"shuffle $shuffleKey partition $id: master lost, use slave")
          committedPartitions.put(id, slavePartition)
        }
      }

      val fileGroups = reducerFileGroupsMap.get(shuffleKey)
      val sets = Array.fill(fileGroups.length)(new util.HashSet[PartitionLocation]())
      committedPartitions.values().foreach { partition =>
        sets(partition.getReduceId).add(partition)
      }
      var i = 0
      while (i < fileGroups.length) {
        fileGroups(i) = sets(i).toArray(new Array[PartitionLocation](0))
        i += 1
      }
    }

    // reply
    if (!dataLost) {
      logInfo(s"succeed to handle stageend! $shuffleKey")
      // record in stageEndShuffleSet
      stageEndShuffleSet.add(shuffleKey)
      if (context != null) {
        context.reply(StageEndResponse(StatusCode.Success, null))
      }
    } else {
      logError(s"failed to handle stageend, lost file! $shuffleKey")
      dataLostShuffleSet.add(shuffleKey)
      // record in stageEndShuffleSet
      stageEndShuffleSet.add(shuffleKey)
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
    // clear for the shuffle
    registeredShuffle.remove(shuffleKey)
    registerShuffleRequest.remove(shuffleKey)
    reducerFileGroupsMap.remove(shuffleKey)
    dataLostShuffleSet.remove(shuffleKey)
    shuffleAllocatedWorkers.remove(shuffleKey)

    logInfo("unregister success")
    context.reply(UnregisterShuffleResponse(StatusCode.Success))
  }

  def handleApplicationLost(context: RpcCallContext, appId: String): Unit = {
    val expiredShuffles = registeredShuffle.filter(_.startsWith(appId))
    expiredShuffles.foreach { key =>
      val splits = key.split("-")
      val appId = splits.dropRight(1).mkString("-")
      val shuffleId = splits.last.toInt
      self.ask(UnregisterShuffle(appId, shuffleId))
    }

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
    workersSnapShot.exists(w => {
      w.containsShuffleMaster(shuffleKey) ||
        w.containsShuffleSlave(shuffleKey)
    })
  }

  private def workersNotBlacklisted(): util.List[WorkerInfo] = {
    workersSnapShot.filter(w => !blacklist.contains(w.hostPort))
  }

  def getWorkerInfos(): String = {
    val sb = new StringBuilder
    workersSnapShot.foreach { w =>
      sb.append("==========WorkerInfos in Master==========\n")
      sb.append(w).append("\n")

      val workerInfo = requestGetWorkerInfos(w.endpoint)
        .workerInfos.asInstanceOf[util.List[WorkerInfo]](0)

      sb.append("==========WorkerInfos in Workers==========\n")
      sb.append(workerInfo).append("\n")

      if (w.hasSameInfoWith(workerInfo)) {
        sb.append("Consist!").append("\n")
      } else {
        sb.append("[ERROR] Inconsist!").append("\n")
      }
    }

    sb.toString()
  }

  def getThreadDump(): String = {
    val sb = new StringBuilder
    val threadDump = Utils.getThreadDump()
    sb.append("==========Master ThreadDump==========\n")
    sb.append(threadDump).append("\n")
    workersSnapShot.foreach(w => {
      sb.append(s"==========Worker ${w.hostPort} ThreadDump==========\n")
      val res = requestThreadDump(w.endpoint)
      sb.append(res.threadDump).append("\n")
    })

    sb.toString()
  }

  private def requestReserveBuffers(
    endpoint: RpcEndpointRef, message: ReserveBuffers): ReserveBuffersResponse = {
    try {
      endpoint.askSync[ReserveBuffersResponse](message)
    } catch {
      case e: Exception =>
        logError("askSync ReserveBuffers failed", e)
        ReserveBuffersResponse(StatusCode.Failed)
    }
  }

  private def requestDestroy(endpoint: RpcEndpointRef, message: Destroy): DestroyResponse = {
    try {
      endpoint.askSync[DestroyResponse](message)
    } catch {
      case e: Exception =>
        logError(s"askSync Destroy failed", e)
        DestroyResponse(StatusCode.Failed, message.masterLocations, message.slaveLocation)
    }
  }

  private def requestCommitFiles(
      endpoint: RpcEndpointRef, message: CommitFiles): CommitFilesResponse = {
    try {
      endpoint.askSync[CommitFilesResponse](message)
    } catch {
      case e: Exception =>
        logError(s"askSync CommitFiles failed", e)
        CommitFilesResponse(StatusCode.Failed, null, null, message.masterIds, message.slaveIds)
    }
  }

  private def requestGetWorkerInfos(endpoint: RpcEndpointRef): GetWorkerInfosResponse = {
    try {
      endpoint.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    } catch {
      case e: Exception =>
        logError(s"askSync GetWorkerInfos failed", e)
        val result = new util.ArrayList[WorkerInfo]
        result.add(new WorkerInfo("unknown", -1, 0, null))
        GetWorkerInfosResponse(StatusCode.Failed, result)
    }
  }

  private def requestThreadDump(endpoint: RpcEndpointRef): ThreadDumpResponse = {
    try {
      endpoint.askSync[ThreadDumpResponse](ThreadDump)
    } catch {
      case e: Exception =>
        logError(s"askSync ThreadDump failed", e)
        ThreadDumpResponse("Unknown")
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
