/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.ess.service.deploy.worker

import java.io.IOException
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.{ConcurrentHashMap, Future, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConversions._

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.exception.AlreadyClosedException
import com.aliyun.emr.ess.common.http.{HttpServer, HttpServerInitializer}
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.metrics.MetricsSystem
import com.aliyun.emr.ess.common.metrics.source.NetWorkSource
import com.aliyun.emr.ess.common.rpc._
import com.aliyun.emr.ess.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.ess.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.ess.protocol.message.ControlMessages._
import com.aliyun.emr.ess.protocol.message.StatusCode
import com.aliyun.emr.ess.service.deploy.worker.http.HttpRequestHandler
import com.aliyun.emr.ess.unsafe.Platform
import com.aliyun.emr.network.TransportContext
import com.aliyun.emr.network.buffer.NettyManagedBuffer
import com.aliyun.emr.network.client.{RpcResponseCallback, TransportClientBootstrap}
import com.aliyun.emr.network.protocol.{PushData, PushMergedData}
import com.aliyun.emr.network.server.TransportServerBootstrap
import io.netty.buffer.ByteBuf

private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    masterRpcAddress: RpcAddress,
    endpointName: String,
    val conf: EssConf,
    val metricsSystem: MetricsSystem)
  extends RpcEndpoint with PushDataHandler with OpenStreamHandler with Logging {

  // init and register master metrics com.aliyun.emr.ess.common.metrics.source
  private val workerSource = {
    val source = new WorkerSource(conf)
    metricsSystem.registerSource(source)
    metricsSystem.registerSource(new NetWorkSource(conf, MetricsSystem.ROLE_WOKRER))
    source
  }

  private val localStorageManager = new LocalStorageManager(conf, workerSource)

  private val (pushServer, pushClientFactory) = {
    val numThreads = conf.getInt("ess.push.io.threads", localStorageManager.numDisks * 2)
    val transportConf = Utils.fromEssConf(conf, "push", numThreads)
    val rpcHandler = new PushDataRpcHandler(transportConf, this)
    val transportContext: TransportContext =
      new TransportContext(transportConf, rpcHandler, false)
    val serverBootstraps: Seq[TransportServerBootstrap] = Nil
    val clientBootstraps: Seq[TransportClientBootstrap] = Nil
    (transportContext.createServer(EssConf.essPushServerPort(conf), serverBootstraps),
      transportContext.createClientFactory(clientBootstraps))
  }

  private val fetchServer = {
    val numThreads = conf.getInt("ess.fetch.io.threads", localStorageManager.numDisks * 2)
    val transportConf = Utils.fromEssConf(conf, "fetch", numThreads)
    val rpcHandler = new ChunkFetchRpcHandler(transportConf, this)
    val transportContext: TransportContext =
      new TransportContext(transportConf, rpcHandler, false)
    val serverBootstraps: Seq[TransportServerBootstrap] = Nil
    transportContext.createServer(EssConf.essFetchServerPort(conf), serverBootstraps)
  }

  private val host = rpcEnv.address.host
  private val port = pushServer.getPort
  private val fetchPort = fetchServer.getPort

  Utils.checkHost(host)
  assert(port > 0)
  assert(fetchPort > 0)

  // whether this Worker registered to Master succesfully
  private val registered = new AtomicBoolean(false)

  private val shuffleMapperAttempts = new ConcurrentHashMap[String, Array[Int]]()

  // master endpoint
  private val masterEndpoint: RpcEndpointRef =
    rpcEnv.setupEndpointRef(masterRpcAddress, RpcNameConstants.MASTER_EP)

  // worker info
  private val workerInfo = new WorkerInfo(host, port, fetchPort,
    EssConf.essWorkerNumSlots(conf, localStorageManager.numDisks), self)

  workerSource.addGauge(WorkerSource.RegisteredShuffleCount, _ => workerInfo.shuffleKeySet().size())
  workerSource.addGauge(WorkerSource.TotalSlots, _ => workerInfo.totalSlots())
  workerSource.addGauge(WorkerSource.SlotsUsed, _ => workerInfo.usedSlots())
  workerSource.addGauge(WorkerSource.SlotsAvailable, _ => workerInfo.freeSlots())

  // Threads
  private val forwardMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")
  private val replicateThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-replicate-data", 64)

  // Configs
  private val HEARTBEAT_MILLIS = EssConf.essWorkerTimeoutMs(conf) / 4

  // shared ExecutorService for flush
  private val commitThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "Worker-CommitFiles", 32)

  override def onStart(): Unit = {
    logInfo(s"Starting Worker $host:$port with ${workerInfo.numSlots} slots")
    registerWithMaster()

    // start heartbeat
    forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        localStorageManager.logAvailableFlushBuffersInfo()
        self.send(SendHeartbeat)
      }
    }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    forwardMessageScheduler.shutdownNow()
    pushServer.close()
  }

  override def receive: PartialFunction[Any, Unit] = {
    case SendHeartbeat =>
      val shuffleKeys = new util.HashSet[String]
      shuffleKeys.addAll(workerInfo.shuffleKeySet())
      shuffleKeys.addAll(localStorageManager.shuffleKeySet())
      val response = masterEndpoint.askSync[HeartbeatResponse](
        HeartbeatFromWorker(host, port, shuffleKeys))
      cleanTaskQueue.put(response.expiredShuffleKeys)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ReserveBuffers(applicationId, shuffleId, masterLocations, slaveLocations) =>
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      workerSource.sample(WorkerSource.ReserveBufferTime, shuffleKey) {
        logInfo(s"Received ReserveBuffers request, $shuffleKey, " +
          s"master partitions: ${masterLocations.map(_.getUniqueId).mkString(",")}; " +
          s"slave partitions: ${slaveLocations.map(_.getUniqueId).mkString(",")}.")
        handleReserveBuffers(context, applicationId, shuffleId, masterLocations, slaveLocations)
        logInfo(s"ReserveBuffers for $shuffleKey succeed.")
      }

    case CommitFiles(shuffleKey, masterIds, slaveIds, mapAttempts) =>
      workerSource.sample(WorkerSource.CommitFilesTime, shuffleKey) {
        logInfo(s"Received CommitFiles request, $shuffleKey, " +
          s"master files ${masterIds.mkString(",")}; slave files ${slaveIds.mkString(",")}.")
        val commitFilesTimeMs = Utils.timeIt({
          handleCommitFiles(context, shuffleKey, masterIds, slaveIds, mapAttempts)
        })
        logInfo(s"Done processed CommitFiles request with shuffleKey $shuffleKey, in " +
          s"${commitFilesTimeMs}ms.")
      }

    case GetWorkerInfos =>
      logInfo("Received GetWorkerInfos request.")
      handleGetWorkerInfos(context)

    case ThreadDump =>
      logInfo("Receive ThreadDump request.")
      handleThreadDump(context)

    case Destroy(shuffleKey, masterLocations, slaveLocations) =>
      logInfo(s"Receive Destroy request, $shuffleKey.")
      handleDestroy(context, shuffleKey, masterLocations, slaveLocations)
  }

  private def handleReserveBuffers(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      masterLocations: util.List[PartitionLocation],
      slaveLocations: util.List[PartitionLocation]): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    val masterPartitions = new util.ArrayList[PartitionLocation]()
    try {
      for (ind <- 0 until masterLocations.size()) {
        val location = masterLocations.get(ind)
        val writer = localStorageManager.createWriter(applicationId, shuffleId, location)
        masterPartitions.add(new WorkingPartition(location, writer))
      }
    } catch {
      case e: Exception =>
        logError(s"CreateWriter for $shuffleKey failed", e)
    }
    if (masterPartitions.size() < masterLocations.size()) {
      logInfo("Not all master partition satisfied, will destroy writers.")
      masterPartitions.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      context.reply(ReserveBuffersResponse(StatusCode.ReserveBufferFailed))
      return
    }

    val slavePartitions = new util.ArrayList[PartitionLocation]()
    try {
      for (ind <- 0 until slaveLocations.size()) {
        val location = slaveLocations.get(ind)
        val writer = localStorageManager.createWriter(applicationId, shuffleId, location)
        slavePartitions.add(new WorkingPartition(location, writer))
      }
    } catch {
      case e: Exception =>
        logError(s"CreateWriter for $shuffleKey failed", e)
    }
    if (slavePartitions.size() < slaveLocations.size()) {
      logError("Not all slave partition satisfied, destroy writers.")
      masterPartitions.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      slavePartitions.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      context.reply(ReserveBuffersResponse(StatusCode.ReserveBufferFailed))
      return
    }

    // reserve success, update status
    workerInfo.addMasterPartitions(shuffleKey, masterPartitions)
    workerInfo.addSlavePartitions(shuffleKey, slavePartitions)
    context.reply(ReserveBuffersResponse(StatusCode.Success))
  }

  private def handleCommitFiles(
      context: RpcCallContext,
      shuffleKey: String,
      masterIds: util.List[String],
      slaveIds: util.List[String],
      mapAttempts: Array[Int]): Unit = {
    // return null if shuffleKey does not exist
    if (!workerInfo.containsShuffle(shuffleKey)) {
      logError(s"Shuffle $shuffleKey doesn't exist!")
      context.reply(CommitFilesResponse(
        StatusCode.ShuffleNotRegistered, null, null, masterIds, slaveIds))
      return
    }

    shuffleMapperAttempts.putIfAbsent(shuffleKey, mapAttempts)

    val committedMasterIds = new util.ArrayList[String]()
    val committedSlaveIds = new util.ArrayList[String]()
    val failedMasterIds = new util.ArrayList[String]()
    val failedSlaveIds = new util.ArrayList[String]()

    val futures = new util.ArrayList[Future[_]]()

    // TODO: refactor these copy/paste code
    if (masterIds != null) {
      masterIds.foreach { id =>
        val target = workerInfo.getMasterLocation(shuffleKey, id)
        if (target != null) {
          val future = commitThreadPool.submit(new Runnable {
            override def run(): Unit = {
              try {
                val bytes = target.asInstanceOf[WorkingPartition].getFileWriter.close()
                if (bytes > 0L) {
                  logInfo(s"FileName ${target.asInstanceOf[WorkingPartition].getFileWriter.getFile.getAbsoluteFile}, size $bytes")
                  committedMasterIds.synchronized {
                    committedMasterIds.add(id)
                  }
                }
              } catch {
                case e: IOException =>
                  logError("[handleCommitFiles] throws IOException", e)
                  failedMasterIds.synchronized {
                    failedMasterIds.add(id)
                  }
              }
            }
          })
          futures.add(future)
        }
      }
    }

    if (slaveIds != null) {
      slaveIds.foreach { id =>
        val target = workerInfo.getSlaveLocation(shuffleKey, id)
        if (target != null) {
          val future = commitThreadPool.submit(new Runnable {
            override def run(): Unit = {
              try {
                val bytes = target.asInstanceOf[WorkingPartition].getFileWriter.close()
                if (bytes > 0L) {
                  logInfo(s"FileName ${target.asInstanceOf[WorkingPartition].getFileWriter.getFile.getAbsoluteFile}, size $bytes")
                  committedSlaveIds.synchronized {
                    committedSlaveIds.add(id)
                  }
                }
              } catch {
                case e: IOException =>
                  logError("[handleCommitFiles] throws IOException", e)
                  failedSlaveIds.synchronized {
                    failedSlaveIds.add(id)
                  }
              }
            }
          })
          futures.add(future)
        }
      }
    }

    masterIds.foreach(id => workerInfo.removeMasterPartition(shuffleKey, id))
    slaveIds.foreach(id => workerInfo.removeSlavePartition(shuffleKey, id))

    futures.foreach(_.get(EssConf.essFlushTimeout(conf), TimeUnit.SECONDS))

    // reply
    if (failedMasterIds.isEmpty && failedSlaveIds.isEmpty) {
      logInfo(s"CommitFiles for $shuffleKey success!")
      context.reply(CommitFilesResponse(
        StatusCode.Success, committedMasterIds, committedSlaveIds, null, null))
    } else {
      logError(s"CommitFiles for $shuffleKey failed!")
      context.reply(CommitFilesResponse(
        StatusCode.PartialSuccess, committedMasterIds, committedSlaveIds,
        failedMasterIds, failedSlaveIds))
    }
  }

  private def handleDestroy(
      context: RpcCallContext,
      shuffleKey: String,
      masterLocations: util.List[String],
      slaveLocations: util.List[String]): Unit = {
    // check whether shuffleKey has registered
    if (!workerInfo.containsShuffleMaster(shuffleKey) &&
      !workerInfo.containsShuffleSlave(shuffleKey)) {
      logWarning(s"[handleDestroy] Shuffle $shuffleKey not registered!")
      context.reply(DestroyResponse(
        StatusCode.ShuffleNotRegistered, masterLocations, slaveLocations))
      return
    }

    val failedMasters = new util.ArrayList[String]()
    val failedSlaves = new util.ArrayList[String]()

    // destroy master locations
    if (masterLocations != null && !masterLocations.isEmpty) {
      masterLocations.foreach { loc =>
        val allocatedLoc = workerInfo.getMasterLocation(shuffleKey, loc)
        if (allocatedLoc == null) {
          failedMasters.add(loc)
        } else {
          allocatedLoc.asInstanceOf[WorkingPartition].getFileWriter.destroy()
        }
      }
      // remove master locations from WorkerInfo
      workerInfo.removeMasterPartitions(shuffleKey, masterLocations)
    }
    // destroy slave locations
    if (slaveLocations != null && !slaveLocations.isEmpty) {
      slaveLocations.foreach { loc =>
        val allocatedLoc = workerInfo.getSlaveLocation(shuffleKey, loc)
        if (allocatedLoc == null) {
          failedSlaves.add(loc)
        } else {
          allocatedLoc.asInstanceOf[WorkingPartition].getFileWriter.destroy()
        }
      }
      // remove slave locations from worker info
      workerInfo.removeSlavePartitions(shuffleKey, slaveLocations)
    }
    // reply
    if (failedMasters.isEmpty && failedSlaves.isEmpty) {
      logInfo("Finished handle destroy.")
      context.reply(DestroyResponse(StatusCode.Success, null, null))
    } else {
      logWarning("Finished handle destroy PartialSuccess.")
      context.reply(DestroyResponse(StatusCode.PartialSuccess, failedMasters, failedSlaves))
    }
  }

  private def handleGetWorkerInfos(context: RpcCallContext): Unit = {
    val list = new util.ArrayList[WorkerInfo]()
    list.add(workerInfo)
    context.reply(GetWorkerInfosResponse(StatusCode.Success, list))
  }

  private def handleThreadDump(context: RpcCallContext): Unit = {
    val threadDump = Utils.getThreadDump()
    context.reply(ThreadDumpResponse(threadDump))
  }

  private def getMapAttempt(body: ByteBuf): (Int, Int) = {
    // header: mapId attemptId batchId compressedTotalSize
    val header = new Array[Byte](8)
    body.getBytes(body.readerIndex(), header)
    val mapId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET)
    val attemptId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET + 4)
    (mapId, attemptId)
  }

  override def handlePushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushData.shuffleKey
    val mode = PartitionLocation.getMode(pushData.mode)
    val body = pushData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.Master

    val key = s"${pushData.requestId}"
    if (isMaster) {
      workerSource.startTimer(WorkerSource.MasterPushDataTime, key)
    } else {
      workerSource.startTimer(WorkerSource.SlavePushDataTime, key)
    }

    val wrappedCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(WorkerSource.MasterPushDataTime, key)
          if (response.remaining() > 0) {
            val resp = ByteBuffer.allocate(response.remaining())
            resp.put(response)
            resp.flip()
            callback.onSuccess(resp)
          } else {
            callback.onSuccess(response)
          }
        } else {
          workerSource.stopTimer(WorkerSource.SlavePushDataTime, key)
          callback.onSuccess(response)
        }
      }

      override def onFailure(e: Throwable): Unit = {
        workerSource.incCounter(WorkerSource.PushDataFailCount)
        callback.onFailure(e)
      }
    }

    // find FileWriter responsible for the data
    val location = if (mode == PartitionLocation.Mode.Master) {
      workerInfo.getMasterLocation(shuffleKey, pushData.partitionUniqueId)
    } else {
      workerInfo.getSlaveLocation(shuffleKey, pushData.partitionUniqueId)
    }

    if (location == null) {
      val (mapId, attemptId) = getMapAttempt(body)
      var msg = ""
      if (shuffleMapperAttempts.containsKey(shuffleKey) && attemptId != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
        msg = s"[handlePushData] Speculation task, ${pushData.partitionUniqueId}, $mode $shuffleKey," +
          s"mapId $mapId, attemptId $attemptId, committed attemptId ${shuffleMapperAttempts.get(shuffleKey)(mapId)}"
        logInfo(msg)
        wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.StageEnded.getValue)))
        return
      } else {
        msg = s"[handlePushData] Partition Location not found!, ${pushData.partitionUniqueId}, $mode $shuffleKey," +
          s"mapId $mapId, attemptId $attemptId"
        logWarning(msg)
      }
      wrappedCallback.onFailure(new IOException(msg))
      return
    }
    val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
    fileWriter.incrementPendingWrites()

    // for master, send data to slave
    if (EssConf.essReplicate(conf) && isMaster) {
      pushData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            val peer = location.getPeer
            val client = pushClientFactory.createClient(
              peer.getHost, peer.getPort, location.getReduceId)
            val newPushData = new PushData(PartitionLocation.Mode.Slave.mode(), shuffleKey,
              pushData.partitionUniqueId, pushData.body)
            client.pushData(newPushData, wrappedCallback)
          } catch {
            case e: Exception =>
              wrappedCallback.onFailure(e)
          }
        }
      })
    } else {
      wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
    }

    try {
      workerSource.sample(WorkerSource.PushDataWriteTime, key) {
        fileWriter.write(body)
      }
    } catch {
      case e: AlreadyClosedException =>
        fileWriter.decrementPendingWrites()
        val (mapId, attemptId) = getMapAttempt(body)
        val endedAttempt = if (shuffleMapperAttempts.containsKey(shuffleKey)) {
          shuffleMapperAttempts.get(shuffleKey)(mapId)
        } else -1
        val msg = s"Append data failed! mapId $mapId, attemptId $attemptId," +
          s"endedAttempt $endedAttempt fileName ${fileWriter.getFile.getAbsolutePath}."
        logWarning(s"$msg ${e.getMessage}")
      case e =>
        logError("[handlePushData] Exception encountered when write", e)
    }
  }

  override def handlePushMergedData(
      pushMergedData: PushMergedData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushMergedData.shuffleKey
    val mode = PartitionLocation.getMode(pushMergedData.mode)
    val batchOffsets = pushMergedData.batchOffsets
    val body = pushMergedData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.Master

    val key = s"${pushMergedData.requestId}"
    if (isMaster) {
      workerSource.startTimer(WorkerSource.MasterPushDataTime, key)
    } else {
      workerSource.startTimer(WorkerSource.SlavePushDataTime, key)
    }

    val wrappedCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(WorkerSource.MasterPushDataTime, key)
          if (response.remaining() > 0) {
            val resp = ByteBuffer.allocate(response.remaining())
            resp.put(response)
            resp.flip()
            callback.onSuccess(resp)
          } else {
            callback.onSuccess(response)
          }
        } else {
          workerSource.stopTimer(WorkerSource.SlavePushDataTime, key)
          callback.onSuccess(response)
        }
      }

      override def onFailure(e: Throwable): Unit = {
        workerSource.incCounter(WorkerSource.PushDataFailCount)
        callback.onFailure(e)
      }
    }

    // find FileWriters responsible for the data
    val locations = pushMergedData.partitionUniqueIds.map { id =>
      val loc = if (mode == PartitionLocation.Mode.Master) {
        workerInfo.getMasterLocation(shuffleKey, id)
      } else {
        workerInfo.getSlaveLocation(shuffleKey, id)
      }
      if (loc == null) {
        val (mapId, attemptId) = getMapAttempt(body)
        var msg = ""
        if (shuffleMapperAttempts.containsKey(shuffleKey) && attemptId != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
          msg = s"[handlePushMergeData] Speculation task, $id, $mode $shuffleKey," +
            s"mapId $mapId, attemptId $attemptId, committed attemptId ${shuffleMapperAttempts.get(shuffleKey)(mapId)}"
          logInfo(msg)
          wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.StageEnded.getValue)))
          return
        } else {
          msg = s"[handlePushMergeData] Partition Location not found!, $id, $mode $shuffleKey," +
            s"mapId $mapId, attemptId $attemptId"
          logWarning(msg)
        }
        wrappedCallback.onFailure(new IOException(msg))
        return
      }
      loc
    }

    val fileWriters = locations.map(_.asInstanceOf[WorkingPartition].getFileWriter)
    fileWriters.foreach(_.incrementPendingWrites())

    // for master, send data to slave
    if (EssConf.essReplicate(conf) && isMaster) {
      pushMergedData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            val location = locations.head
            val peer = location.getPeer
            val client = pushClientFactory.createClient(
              peer.getHost, peer.getPort, location.getReduceId)
            val newPushMergedData = new PushMergedData(PartitionLocation.Mode.Slave.mode(),
              shuffleKey, pushMergedData.partitionUniqueIds, batchOffsets, pushMergedData.body)
            client.pushMergedData(newPushMergedData, wrappedCallback)
          } catch {
            case e: Exception =>
              wrappedCallback.onFailure(e)
          }
        }
      })
    } else {
      wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
    }

    var index = 0
    var fileWriter: FileWriter = null
    var alreadyClosed = false
    while (index < fileWriters.length) {
      fileWriter = fileWriters(index)
      val offset = body.readerIndex() + batchOffsets(index)
      val length = if (index == fileWriters.length - 1) {
        body.readableBytes() - batchOffsets(index)
      } else {
        batchOffsets(index + 1) - batchOffsets(index)
      }
      val batchBody = body.slice(offset, length)

      try {
        if (!alreadyClosed) {
          fileWriter.write(batchBody)
        } else {
          fileWriter.decrementPendingWrites()
        }
      } catch {
        case e: AlreadyClosedException =>
          fileWriter.decrementPendingWrites()
          alreadyClosed = true
          val (mapId, attemptId) = getMapAttempt(body)
          val endedAttempt = if (shuffleMapperAttempts.containsKey(shuffleKey)) {
            shuffleMapperAttempts.get(shuffleKey)(mapId)
          } else -1
          val msg = s"[handlePushMergedData] Append data failed! mapId $mapId, attemptId" +
            s"$attemptId, endedAttempt $endedAttempt fileName ${fileWriter.getFile.getAbsolutePath}."
          logWarning(s"$msg ${e.getMessage}")
        case e: Exception =>
          logError("[handlePushMergedData] Exception encountered when write", e)
      }
      index += 1
    }
  }

  override def handleOpenStream(
      shuffleKey: String, fileName: String): OpenStreamHandler.FileInfo = {
    // find FileWriter responsible for the data
    val fileWriter = localStorageManager.getWriter(shuffleKey, fileName)
    if (fileWriter eq null) {
      val msg = s"File not found! $shuffleKey, $fileName"
      logError(msg)
      return null
    }
    new OpenStreamHandler.FileInfo(
      fileWriter.getFile, fileWriter.getChunkOffsets, fileWriter.getFileLength)
  }

  private def registerWithMaster() {
    logInfo("Trying to register with master.")
    var res = masterEndpoint.askSync[RegisterWorkerResponse](
      RegisterWorker(host, port, fetchPort, workerInfo.numSlots, self)
    )
    var registerTimeout = EssConf.essRegisterWorkerTimeoutMs(conf)
    val delta = 2000
    while (!res.success && registerTimeout > 0) {
      logInfo(s"Register worker failed!, ${res.message}")
      Thread.sleep(delta)
      registerTimeout = registerTimeout - delta
      logInfo("Trying to re-register with master.")
      res = masterEndpoint.askSync[RegisterWorkerResponse](
        RegisterWorker(host, port, fetchPort, workerInfo.numSlots, self)
      )
    }
    logInfo("Registered worker successfully")

    // Registered successfully
    registered.set(true)
  }

  private val cleanTaskQueue = new LinkedBlockingQueue[util.HashSet[String]]
  private val cleaner = new Thread("Cleaner") {
    override def run(): Unit = {
      while (true) {
        val expiredShuffleKeys = cleanTaskQueue.take()
        try {
          cleanup(expiredShuffleKeys)
        } catch {
          case e: Exception =>
            logError("Cleanup failed", e)
        }
      }
    }
  }
  cleaner.setDaemon(true)
  cleaner.start()

  private def cleanup(expiredShuffleKeys: util.HashSet[String]): Unit = {
    expiredShuffleKeys.foreach { shuffleKey =>
      workerInfo.getAllMasterLocations(shuffleKey).foreach { partition =>
        partition.asInstanceOf[WorkingPartition].getFileWriter.destroy()
      }
      workerInfo.getAllSlaveLocations(shuffleKey).foreach { partition =>
        partition.asInstanceOf[WorkingPartition].getFileWriter.destroy()
      }
      workerInfo.removeMasterPartitions(shuffleKey)
      workerInfo.removeSlavePartitions(shuffleKey)
      shuffleMapperAttempts.remove(shuffleKey)
    }

    localStorageManager.cleanup(expiredShuffleKeys)
  }

  def Registered(): Boolean = {
    registered.get()
  }
}

private[deploy] object Worker extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new EssConf
    val workerArgs = new WorkerArguments(args, conf)

    val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, WorkerSource.ServletPath)

    val rpcEnv = RpcEnv.create(RpcNameConstants.WORKER_SYS,
      workerArgs.host,
      workerArgs.port,
      conf)

    val masterAddresses = RpcAddress.fromEssURL(workerArgs.master)
    rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP,
      new Worker(rpcEnv, masterAddresses, RpcNameConstants.WORKER_EP, conf, metricsSystem))

    if (EssConf.essMetricsSystemEnable(conf)) {
      logInfo(s"Metrics system enabled!")
      metricsSystem.start()

      var port = EssConf.essWorkerPrometheusMetricPort(conf)
      var initialized = false
      while (!initialized) {
        try {
          val httpServer = new HttpServer(
            new HttpServerInitializer(new HttpRequestHandler(metricsSystem.getPrometheusHandler)), port)
          httpServer.start()
          initialized = true
        } catch {
          case e: Exception =>
            logWarning(s"HttpServer port $port may already exist, try port ${port+1}.", e)
            port += 1
            Thread.sleep(1000)
        }
      }
      logInfo(s"HttpServer on port $port started.")
    }

    rpcEnv.awaitTermination()
  }
}
