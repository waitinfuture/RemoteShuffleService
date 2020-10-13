package com.aliyun.emr.ess.service.deploy.master

import java.util
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConversions._

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.http.{HttpServer, HttpServerInitializer}
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.metrics.MetricsSystem
import com.aliyun.emr.ess.common.rpc._
import com.aliyun.emr.ess.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.ess.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.ess.protocol.message.ControlMessages._
import com.aliyun.emr.ess.protocol.message.StatusCode
import com.aliyun.emr.ess.service.deploy.master.http.HttpRequestHandler
import com.aliyun.emr.ess.service.deploy.worker.WorkerInfo
import io.netty.util.internal.ConcurrentSet

private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    val conf: EssConf,
    val metricsSystem: MetricsSystem)
  extends RpcEndpoint with Logging {

  type WorkerResource = java.util.Map[WorkerInfo,
    (java.util.List[PartitionLocation], java.util.List[PartitionLocation])]

  // Threads
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")
  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _
  private var checkForApplicationTimeOutTask: ScheduledFuture[_] = _
  private var checkForShuffleRemoval: ScheduledFuture[_] = _
  private val nonEagerHandler = ThreadUtils.newDaemonCachedThreadPool("master-noneager-handler", 64)

  // Config constants
  private val WorkerTimeoutMs = EssConf.essWorkerTimeoutMs(conf)
  private val ApplicationTimeoutMs = EssConf.essApplicationTimeoutMs(conf)
  private val RemoveShuffleDelayMs = EssConf.essRemoveShuffleDelayMs(conf)
  private val ShouldReplicate = EssConf.essReplicate(conf)

  // States
  private val workers = new util.ArrayList[WorkerInfo]()
  private def workersSnapShot = workers.synchronized(new util.ArrayList[WorkerInfo](workers))

  private val appHeartbeatTime = new util.HashMap[String, Long]()
  private val unregisterShuffleTime = new ConcurrentHashMap[String, Long]()
  private val hostnameSet = new ConcurrentSet[String]()

  // key: "appId_shuffleId"
  private val registeredShuffle = new ConcurrentSet[String]()
  private val shuffleMapperAttempts = new ConcurrentHashMap[String, Array[Int]]()
  private val reducerFileGroupsMap =
    new ConcurrentHashMap[String, Array[Array[PartitionLocation]]]()
  private val dataLostShuffleSet = new ConcurrentSet[String]()
  private val stageEndShuffleSet = new ConcurrentSet[String]()
  private val shuffleAllocatedWorkers = new ConcurrentHashMap[String, ConcurrentSet[WorkerInfo]]()

  // init and register master metrics com.aliyun.emr.ess.common.metrics.source
  private val masterSource = {
    val source = new MasterSource(conf)
    source.addGauge(MasterSource.RegisteredShuffleCount, _ => registeredShuffle.size() - unregisterShuffleTime.size())
    source.addGauge(MasterSource.BlacklistedWorkerCount, _ => blacklist.size())
    // TODO look into the error  err="strconv.ParseFloat: parsing..."
//    source.addGauge[String](MasterSource.SHUFFLE_MANAGER_HOSTNAME_LIST, _ => getHostnameList())
    metricsSystem.registerSource(source)
    source
  }

  // revive request waiting for response
  // shuffleKey -> (partitionId -> set)
  private val reviving =
    new ConcurrentHashMap[String, ConcurrentHashMap[Integer, util.Set[RpcCallContext]]]()

  // register shuffle request waiting for response
  private val registerShuffleRequest = new ConcurrentHashMap[String, util.Set[RpcCallContext]]()

  // blacklist
  private val blacklist = new ConcurrentSet[String]()

  // workerLost events
  private val workerLostEvents = new ConcurrentSet[String]()

  // start threads to check timeout for workers and applications
  override def onStart(): Unit = {
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WorkerTimeoutMs, TimeUnit.MILLISECONDS)

    checkForApplicationTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForApplicationTimeOut)
      }
    }, 0, ApplicationTimeoutMs / 2, TimeUnit.MILLISECONDS)
    
    checkForShuffleRemoval = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(RemoveExpiredShuffle)
      }
    }, 0, RemoveShuffleDelayMs, TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    if (checkForApplicationTimeOutTask != null) {
      checkForApplicationTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"Client $address got disassociated.")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case CheckForWorkerTimeOut =>
      timeoutDeadWorkers()
    case CheckForApplicationTimeOut =>
      timeoutDeadApplications()
    case RemoveExpiredShuffle =>
      removeExpiredShuffle()
    case WorkerLost(host, port) =>
      logInfo(s"Received WorkerLost, $host:$port.")
      handleWorkerLost(null, host, port)
    case HeartBeatFromApplication(appId) =>
      handleHeartBeatFromApplication(appId)
    case StageEnd(applicationId, shuffleId) =>
      logInfo(s"Received StageEnd request, ${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleStageEnd(null, applicationId, shuffleId)
    case UnregisterShuffle(applicationId, shuffleId) =>
      logInfo(s"Received UnregisterShuffle request, ${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleUnregisterShuffle(null, applicationId, shuffleId)
  }

  def reserveBuffers(
      applicationId: String, shuffleId: Int, slots: WorkerResource): util.List[WorkerInfo] = {
    val failed = new util.ArrayList[WorkerInfo]()

    slots.foreach { entry =>
      val res = requestReserveBuffers(entry._1.endpoint,
        ReserveBuffers(applicationId, shuffleId, entry._2._1, entry._2._2))
      if (res.status.equals(StatusCode.Success)) {
        logInfo(s"Successfully allocated partitions buffer from worker ${entry._1.hostPort}.")
      } else {
        logError(s"[reserveBuffers] Failed to reserve buffers from worker ${entry._1.hostPort}.")
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
      logInfo("Reserve buffers failed, retry once.")
      reserveBuffers(applicationId, shuffleId, slots.filterKeys(worker => failed.contains(worker)))
    } else null

    // add into blacklist
    if (failed != null) {
      failed.foreach(w => blacklist.add(w.hostPort))
      masterSource.incCounter(MasterSource.BlacklistedWorkerCount, failed.size())
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
    case RegisterWorker(host, port, fetchPort, numSlots, worker) =>
      logInfo(s"Received RegisterWorker request, $host:$port $numSlots.")
      handleRegisterWorker(context, host, port, fetchPort, numSlots, worker)

    case RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions, hostname) =>
      logDebug(s"Received RegisterShuffle request, " +
        s"$applicationId, $shuffleId, $numMappers, $numPartitions.")
      handleRegisterShuffle(context, applicationId, shuffleId, numMappers, numPartitions, hostname)

    case Revive(applicationId, shuffleId, mapId, attemptId, reduceId, epoch, oldPartition) =>
      val key = s"${Utils.makeShuffleKey(applicationId, shuffleId)}, $mapId-$attemptId-$reduceId-$epoch"
      logInfo(s"Received Revive request, key: $key oldPartition: $oldPartition.")
      masterSource.sample(MasterSource.ReviveTime, key) {
        handleRevive(context, applicationId, shuffleId, mapId, attemptId,
          reduceId, epoch, oldPartition)
      }

    case MapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers) =>
      logDebug(s"Received MapperEnd request, " +
        s"${Utils.makeMapKey(applicationId, shuffleId, mapId, attemptId)}.")
      handleMapperEnd(context, applicationId, shuffleId, mapId, attemptId, numMappers)

    case GetReducerFileGroup(applicationId: String, shuffleId: Int) =>
      logDebug(s"Received GetShuffleFileGroup request, ${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleGetReducerFileGroup(context, applicationId, shuffleId)

    case GetWorkerInfos =>
      logInfo("Received GetWorkerInfos request")
      handleGetWorkerInfos(context)

    case StageEnd(applicationId, shuffleId) =>
      logInfo(s"Received StageEnd request, ${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleStageEnd(context, applicationId, shuffleId)

    case WorkerLost(host, port) =>
      logInfo(s"Received WorkerLost request, $host:$port.")
      handleWorkerLost(context, host, port)

    case MasterPartitionSuicide(shuffleKey, location) =>
      logInfo(s"Received MasterPartitionSuicide request, $location.")
      handleMasterPartitionSuicide(context, shuffleKey, location)

    case ApplicationLost(appId) =>
      logInfo(s"Received ApplicationLost request, $appId.")
      handleApplicationLost(context, appId)

    case HeartbeatFromWorker(host, port, shuffleKeys) =>
      handleHeartBeatFromWorker(context, host, port, shuffleKeys)
  }

  private def timeoutDeadWorkers() {
    val currentTime = System.currentTimeMillis()
    var ind = 0
    workersSnapShot.foreach { worker =>
      if (worker.lastHeartbeat < currentTime - WorkerTimeoutMs
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
    logInfo("Check for timeout dead applications.")
    val currentTime = System.currentTimeMillis()
    val keys = appHeartbeatTime.keySet().toList
    keys.foreach { key =>
      if (appHeartbeatTime.get(key) < currentTime - ApplicationTimeoutMs) {
        logWarning(s"Application $key timeout, trigger applicationLost event.")
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
  
  private def removeExpiredShuffle(): Unit = {
    logInfo("Check for expired shuffle.")
    val currentTime = System.currentTimeMillis()
    val keys = unregisterShuffleTime.keys().toList
    keys.foreach { key =>
      if (unregisterShuffleTime.get(key) < currentTime - RemoveShuffleDelayMs) {
        logInfo(s"Clear shuffle $key.")
        // clear for the shuffle
        registeredShuffle.remove(key)
        registerShuffleRequest.remove(key)
        reducerFileGroupsMap.remove(key)
        dataLostShuffleSet.remove(key)
        shuffleAllocatedWorkers.remove(key)
        shuffleMapperAttempts.remove(key)
        stageEndShuffleSet.remove(key)
        reviving.remove(key)
        unregisterShuffleTime.remove(key)
      }
    }
  }

  private def handleHeartBeatFromWorker(
      context: RpcCallContext,
      host: String,
      port: Int,
      shuffleKeys: util.HashSet[String]): Unit = {
    logDebug(s"Received heartbeat from $host:$port.")
    val worker: WorkerInfo = workersSnapShot.find(w => w.host == host && w.port == port).orNull
    if (worker == null) {
      logInfo(s"Received heartbeat from unknown worker! $host:$port.")
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
        logWarning(s"Shuffle $shuffleKey expired on $host:$port.")
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
      logError(s"Worker not found for the location $location!")
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
      fetchPort: Int,
      numSlots: Int,
      workerRef: RpcEndpointRef): Unit = {
    logInfo(s"Registering worker $host:$port with $numSlots slots.")
    val hostPort = host + ":" + port;
    if (workersSnapShot.exists(w => w.hostPort == hostPort)) {
      logWarning("Receive RegisterWorker while worker already exists, trigger WorkerLost")
      workerLostEvents.synchronized {
        if (!workerLostEvents.contains(hostPort)) {
          workerLostEvents.add(hostPort)
          self.send(WorkerLost(host, port))
        }
      }
      context.reply(RegisterWorkerResponse(false, "Worker already registered!"))
    } else if (workerLostEvents.contains(hostPort)) {
      logWarning("Receive RegisterWorker while worker in workerLostEvents.")
      context.reply(RegisterWorkerResponse(false, "Worker in workerLostEvents."))
    } else {
      val worker = new WorkerInfo(host, port, fetchPort, numSlots, workerRef)
      workers.synchronized {
        workers.add(worker)
      }
      logInfo(s"Registered worker $worker.")
      context.reply(RegisterWorkerResponse(true, null))
    }
  }

  def handleRegisterShuffle(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int,
      hostname: String): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)

    if (hostname != null) {
      hostnameSet.add(hostname)
    }

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
          val initialLocs = workersSnapShot
            .flatMap(w => w.getAllMasterLocationsWithMinEpoch(shuffleKey))
            .filter(_.getEpoch == 0)
          logDebug(s"Shuffle $shuffleKey already registered, just return.")
          if (initialLocs.size != numPartitions) {
            logWarning(s"Shuffle $shuffleKey location size ${initialLocs.size} not equal to " +
              s"numPartitions: $numPartitions!")
          }
          context.reply(RegisterShuffleResponse(StatusCode.Success, initialLocs))
          return
        }
        logInfo(s"New shuffle request, shuffleKey $shuffleKey.")
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
        (0 until numPartitions).map(new Integer(_)).toList,
        ShouldReplicate
      )
    }

    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError(s"OfferSlots for $shuffleKey failed!")
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
      logWarning("Reserve buffers still fail after retrying, clear buffers.")
      slots.foreach(entry => {
        destroyBuffersWithRetry(shuffleKey, entry._1,
          entry._2._1.map(_.getUniqueId),
          entry._2._2.map(_.getUniqueId))
        entry._1.removeMasterPartitions(shuffleKey, entry._2._1.map(_.getUniqueId))
        entry._1.removeSlavePartitions(shuffleKey, entry._2._2.map(_.getUniqueId))
      })
      logError(s"RegisterShuffle for $shuffleKey failed, reply to all.")
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
    registeredShuffle.add(shuffleKey)
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

    logInfo(s"Handle RegisterShuffle Success for $shuffleKey.")
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
      mapId: Int,
      attemptId: Int,
      reduceId: Int,
      oldEpoch: Int,
      oldPartition: PartitionLocation): Unit = {
    // check whether shuffle has registered
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    if (!registeredShuffle.contains(shuffleKey)) {
      logError(s"[handleRevive] shuffle $shuffleKey not registered!")
      context.reply(ReviveResponse(StatusCode.ShuffleNotRegistered, null))
      return
    }
    if (shuffleMapperAttempts.containsKey(shuffleKey)
      && shuffleMapperAttempts.get(shuffleKey)(mapId) != -1) {
      logWarning(s"[handleRevive] Mapper ended, mapId $mapId, current attemptId $attemptId, " +
        s"ended attemptId ${shuffleMapperAttempts.get(shuffleKey)(mapId)}, shuffleKey $shuffleKey.")
      context.reply(ReviveResponse(StatusCode.MapEnded, null))
      return
    }

    def checkAndBlacklist(hostPort: String): Unit = {
      val worker = workersSnapShot.find(w => w.hostPort == hostPort).orNull
      if (worker != null && !worker.isActive()) {
        logWarning(s"Add ${worker.hostPort} to blacklist")
        blacklist.add(hostPort)
        masterSource.incCounter(MasterSource.BlacklistedWorkerCount)
      }
    }

    val candidateWorkers = if (oldPartition != null) {
      // check to see if partition can be reached. if not, add into blacklist
      val hostPort = oldPartition.hostPort()
      checkAndBlacklist(hostPort)
      if (oldPartition.getPeer != null) {
        val peerHostPort = oldPartition.getPeer.hostPort()
        checkAndBlacklist(peerHostPort)
        workersNotBlacklisted(Set(hostPort, peerHostPort))
      } else {
        workersNotBlacklisted(Set(hostPort))
      }
    } else {
      workersNotBlacklisted()
    }

    // check if there exists request for the partition, if do just register
    val newMapFunc =
      new java.util.function.Function[String, ConcurrentHashMap[Integer, util.Set[RpcCallContext]]]() {
        override def apply(s: String): ConcurrentHashMap[Integer, util.Set[RpcCallContext]] =
          new ConcurrentHashMap()
      }
    val shuffleReviving = reviving.computeIfAbsent(shuffleKey, newMapFunc)
    shuffleReviving.synchronized {
      if (shuffleReviving.containsKey(reduceId)) {
        shuffleReviving.get(reduceId).add(context)
        logInfo(s"For $shuffleKey, same partition $reduceId-$oldEpoch is reviving, register context.")
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
          logInfo(s"New partition found, old partition $reduceId-$oldEpoch return it $shuffleKey " + currentLocation)
          return
        }
        // no newer partition, register and allocate
        val set = new util.HashSet[RpcCallContext]()
        set.add(context)
        shuffleReviving.put(reduceId, set)
      }
    }

    // offer new slot
    val slots = workers.synchronized {
      MasterUtil.offerSlots(shuffleKey,
        // avoid offer slots on the same host of oldPartition
        candidateWorkers,
        Seq(new Integer(reduceId)),
        Array(oldEpoch),
        ShouldReplicate
      )
    }
    // reply false if offer slots failed
    if (slots == null || slots.isEmpty()) {
      logError("[handleRevive] offerSlot failed.")
      shuffleReviving.synchronized {
        val set = shuffleReviving.get(reduceId)
        set.foreach(_.reply(ReviveResponse(StatusCode.SlotNotAvailable, null)))
        shuffleReviving.remove(reduceId)
      }
      return
    }
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
      logError(s"Revive reserve buffers failed for $shuffleKey.")
      shuffleReviving.synchronized {
        val set = shuffleReviving.get(reduceId)
        set.foreach(_.reply(ReviveResponse(StatusCode.ReserveBufferFailed, null)))
        shuffleReviving.remove(reduceId)
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
    logInfo(s"Revive reserve buffer success for $shuffleKey $location.")
    shuffleReviving.synchronized {
      val set = shuffleReviving.get(reduceId)
      set.foreach(_.reply(ReviveResponse(StatusCode.Success, location)))
      shuffleReviving.remove(reduceId)
      logInfo(s"Reply and remove $shuffleKey $reduceId partition success.")
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
      // it would happen when task with no shuffle data called MapperEnd first
      if (attempts == null) {
        logInfo(s"[handleMapperEnd] $shuffleKey not registered, create one.")
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
      logInfo(s"Last MapperEnd, call StageEnd with shuffleKey: ${Utils.makeShuffleKey(applicationId, shuffleId)}.")
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
      logInfo(s"Wait for StageEnd, $shuffleKey.")
      Thread.sleep(50)
      if (timeout <= 0) {
        logError(s"StageEnd Timeout! $shuffleKey.")
        context.reply(GetReducerFileGroupResponse(StatusCode.Failed, null, null))
        return
      }
      timeout = timeout - delta
    }

    if (dataLostShuffleSet.contains(shuffleKey)) {
      context.reply(GetReducerFileGroupResponse(StatusCode.Failed, null, null))
    } else {
      val shuffleFileGroup = reducerFileGroupsMap.get(shuffleKey)
      context.reply(GetReducerFileGroupResponse(
        StatusCode.Success,
        shuffleFileGroup,
        shuffleMapperAttempts.get(shuffleKey)
      ))
    }
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
      logInfo(s"[handleStageEnd] $shuffleKey not registered, maybe no shuffle data within this stage.")
      // record in stageEndShuffleSet
      stageEndShuffleSet.add(shuffleKey)
      if (context != null) {
        context.reply(StageEndResponse(StatusCode.ShuffleNotRegistered, null))
      }
      return
    }

    // check connection before commit files
    workersSnapShot.foreach { w =>
      if (!w.isActive() && !workerLostEvents.contains(w.hostPort)) {
        logInfo(s"Find WorkerLost in StageEnd ${w.hostPort}.")
        self.send(WorkerLost(w.host, w.port))
        workerLostEvents.add(w.hostPort)
      }
    }

    // wait until all workerLost events are handled
    while (!workerLostEvents.isEmpty) {
      Thread.sleep(50)
      logWarning("Wait for WorkerLost events all handled.")
    }

    // ask allLocations workers holding partitions to commit files
    val masterPartMap = new ConcurrentHashMap[String, PartitionLocation]
    val slavePartMap = new ConcurrentHashMap[String, PartitionLocation]
    val committedMasterIds = new ConcurrentSet[String]
    val committedSlaveIds = new ConcurrentSet[String]
    val failedMasterIds = new ConcurrentSet[String]
    val failedSlaveIds = new ConcurrentSet[String]

    val allocatedWorkers = shuffleAllocatedWorkers.get(shuffleKey)

    val parallelism = Math.min(workersSnapShot.size(), EssConf.essRpcMaxParallelism(conf))
    ThreadUtils.parmap(
      allocatedWorkers.to, "CommitFiles", parallelism) { worker =>
      if (worker.containsShuffle(shuffleKey)) {
        val masterParts = worker.getAllMasterLocations(shuffleKey)
        val slaveParts = worker.getAllSlaveLocations(shuffleKey)
        masterParts.foreach { p =>
          val partition = new PartitionLocation(p)
          partition.setPort(worker.fetchPort)
          partition.setPeer(null)
          masterPartMap.put(partition.getUniqueId, partition)
        }
        slaveParts.foreach { p =>
          val partition = new PartitionLocation(p)
          partition.setPort(worker.fetchPort)
          partition.setPeer(null)
          slavePartMap.put(partition.getUniqueId, partition)
        }

        val masterIds = masterParts.map(_.getUniqueId)
        val slaveIds = slaveParts.map(_.getUniqueId)

        val start = System.currentTimeMillis()
        val res = requestCommitFiles(worker.endpoint, CommitFiles(shuffleKey, masterIds, slaveIds,
          shuffleMapperAttempts.get(shuffleKey)))

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
      if (!ShouldReplicate && failedMasterIds.nonEmpty) {
        return true
      }
      for (id <- failedMasterIds) {
        if(failedSlaveIds.contains(id)) {
          logError(s"For $shuffleKey partition $id: data lost.")
          return true
        }
      }
      false
    }

    val dataLost = hasCommonFailedIds()

    if (!dataLost) {
      val committedPartitions = new util.HashMap[String, PartitionLocation]
      committedMasterIds.foreach { id =>
        committedPartitions.put(id, masterPartMap.get(id))
      }
      committedSlaveIds.foreach { id =>
        val slavePartition = slavePartMap.get(id)
        val masterPartition = committedPartitions.get(id)
        if (masterPartition ne null) {
          masterPartition.setPeer(slavePartition)
          slavePartition.setPeer(masterPartition)
        } else {
          logWarning(s"Shuffle $shuffleKey partition $id: master lost, " +
            s"use slave $slavePartition.")
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
      logInfo(s"Succeed to handle stageEnd for $shuffleKey.")
      // record in stageEndShuffleSet
      stageEndShuffleSet.add(shuffleKey)
      if (context != null) {
        context.reply(StageEndResponse(StatusCode.Success, null))
      }
    } else {
      logError(s"Failed to handle stageEnd for $shuffleKey, lost file!")
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
      logInfo(s"Call StageEnd before Unregister Shuffle $shuffleKey.")
      handleStageEnd(null, appId, shuffleId)
    }

    if (partitionExists(shuffleKey)) {
      logWarning(s"Partition exists for shuffle $shuffleKey, " +
        "maybe caused by task rerun or speculative.")
      workersSnapShot.foreach { worker =>
        worker.removeMasterPartitions(shuffleKey)
        worker.removeSlavePartitions(shuffleKey)
      }
    }

    // add shuffleKey to delay shuffle removal set
    unregisterShuffleTime.put(shuffleKey, System.currentTimeMillis())

    logInfo(s"Unregister for $shuffleKey success.")
    if (context != null) {
      context.reply(UnregisterShuffleResponse(StatusCode.Success))
    }
  }

  def handleApplicationLost(context: RpcCallContext, appId: String): Unit = {
    nonEagerHandler.submit(new Runnable {
      override def run(): Unit = {
        val expiredShuffles = registeredShuffle.filter(_.startsWith(appId))
        expiredShuffles.foreach { key =>
          val splits = key.split("-")
          val appId = splits.dropRight(1).mkString("-")
          val shuffleId = splits.last.toInt
          handleUnregisterShuffle(null, appId, shuffleId)
        }

        logInfo("Finished handling applicationLost.")
        context.reply(ApplicationLostResponse(StatusCode.Success))
      }
    })
  }

  private def handleHeartBeatFromApplication(appId: String): Unit = {
    appHeartbeatTime.synchronized {
      logInfo(s"Heartbeat from application, appId $appId.")
      appHeartbeatTime.put(appId, System.currentTimeMillis())
    }
  }

  private def partitionExists(shuffleKey: String): Boolean = {
    workersSnapShot.exists(w => {
      w.containsShuffleMaster(shuffleKey) ||
        w.containsShuffleSlave(shuffleKey)
    })
  }

  private def workersNotBlacklisted(
      tmpBlacklist: Set[String] = Set.empty): util.List[WorkerInfo] = {
    workersSnapShot.filter(
      w => !blacklist.contains(w.hostPort) && !tmpBlacklist.contains(w.hostPort))
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
        sb.append("[ERROR] Inconsistent!").append("\n")
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

  def getHostnameList(): String = {
    hostnameSet.mkString(",")
  }

  private def requestReserveBuffers(
    endpoint: RpcEndpointRef, message: ReserveBuffers): ReserveBuffersResponse = {
    val shuffleKey = Utils.makeShuffleKey(message.applicationId, message.shuffleId)
    masterSource.sample(MasterSource.ReserveBufferTime, shuffleKey) {
      try {
        endpoint.askSync[ReserveBuffersResponse](message)
      } catch {
        case e: Exception =>
          logError(s"AskSync ReserveBuffers for $shuffleKey failed.", e)
          ReserveBuffersResponse(StatusCode.Failed)
      }
    }
  }

  private def requestDestroy(endpoint: RpcEndpointRef, message: Destroy): DestroyResponse = {
    try {
      endpoint.askSync[DestroyResponse](message)
    } catch {
      case e: Exception =>
        logError(s"AskSync Destroy for ${message.shuffleKey} failed.", e)
        DestroyResponse(StatusCode.Failed, message.masterLocations, message.slaveLocation)
    }
  }

  private def requestCommitFiles(
      endpoint: RpcEndpointRef, message: CommitFiles): CommitFilesResponse = {
    masterSource.sample(MasterSource.CommitFilesTime, message.shuffleKey) {
      try {
        endpoint.askSync[CommitFilesResponse](message)
      } catch {
        case e: Exception =>
          logError(s"AskSync CommitFiles for ${message.shuffleKey} failed.", e)
          CommitFilesResponse(StatusCode.Failed, null, null, message.masterIds, message.slaveIds)
      }
    }
  }

  private def requestGetWorkerInfos(endpoint: RpcEndpointRef): GetWorkerInfosResponse = {
    try {
      endpoint.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    } catch {
      case e: Exception =>
        logError(s"AskSync GetWorkerInfos failed.", e)
        val result = new util.ArrayList[WorkerInfo]
        result.add(new WorkerInfo("unknown", -1, -1, 0, null))
        GetWorkerInfosResponse(StatusCode.Failed, result)
    }
  }

  private def requestThreadDump(endpoint: RpcEndpointRef): ThreadDumpResponse = {
    try {
      endpoint.askSync[ThreadDumpResponse](ThreadDump)
    } catch {
      case e: Exception =>
        logError(s"AskSync ThreadDump failed.", e)
        ThreadDumpResponse("Unknown")
    }
  }
}

private[deploy] object Master extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new EssConf()

    val metricsSystem = MetricsSystem.createMetricsSystem("master", conf, MasterSource.ServletPath)

    val masterArgs = new MasterArguments(args, conf)
    val rpcEnv = RpcEnv.create(
      RpcNameConstants.MASTER_SYS,
      masterArgs.host,
      masterArgs.port,
      conf)
    val master = new Master(rpcEnv, rpcEnv.address, conf, metricsSystem)
    rpcEnv.setupEndpoint(RpcNameConstants.MASTER_EP, master)

    val handlers = if (EssConf.essMetricsSystemEnable(conf)) {
      logInfo(s"Metrics system enabled.")
      metricsSystem.start()
      new HttpRequestHandler(master, metricsSystem.getPrometheusHandler)
    } else {
      new HttpRequestHandler(master, null)
    }

    val httpServer = new HttpServer(new HttpServerInitializer(handlers),
      EssConf.essMasterPrometheusMetricPort(conf))
    httpServer.start()
    logInfo("[Master] httpServer started.")

    rpcEnv.awaitTermination()
  }
}
