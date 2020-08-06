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
import java.util.concurrent.{Future, LinkedBlockingQueue, TimeUnit}

import scala.collection.JavaConversions._

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.rpc._
import com.aliyun.emr.ess.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.ess.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.ess.protocol.message.ControlMessages._
import com.aliyun.emr.ess.protocol.message.StatusCode
import com.aliyun.emr.network.TransportContext
import com.aliyun.emr.network.buffer.NettyManagedBuffer
import com.aliyun.emr.network.client.{RpcResponseCallback, TransportClientBootstrap}
import com.aliyun.emr.network.protocol.PushData
import com.aliyun.emr.network.server.TransportServerBootstrap

private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    masterRpcAddress: RpcAddress,
    endpointName: String,
    val conf: EssConf)
  extends RpcEndpoint with DataHandler with Logging {

  private val (dataServer, dataClientFactory) = {
    val transportConf = Utils.fromEssConf(conf, "data", conf.getInt("ess.data.io.threads", 0))
    val rpcHandler = new DataRpcHandler(transportConf, this)
    val transportContext: TransportContext =
      new TransportContext(transportConf, rpcHandler, true)
    val serverBootstraps: Seq[TransportServerBootstrap] = Nil
    val clientBootstraps: Seq[TransportClientBootstrap] = Nil
    (transportContext.createServer(serverBootstraps),
      transportContext.createClientFactory(clientBootstraps))
  }

  private val host = rpcEnv.address.host
  private val port = dataServer.getPort

  Utils.checkHost(host)
  assert(port > 0)

  // master endpoint
  private val masterEndpoint: RpcEndpointRef =
    rpcEnv.setupEndpointRef(masterRpcAddress, RpcNameConstants.MASTER_EP)

  // worker info
  private val workerInfo = new WorkerInfo(host, port, EssConf.essWorkerNumSlots(conf), self)

  private val localStorageManager = new LocalStorageManager(conf)

  // Threads
  private val forwardMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

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
    dataServer.close()
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (masterRpcAddress == remoteAddress) {
      logInfo(s"Master $remoteAddress Disassociated !")
      reRegisterWithMaster()
    }
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
      logInfo(s"received ReserveBuffers request, $applicationId $shuffleId")
      handleReserveBuffers(context, applicationId, shuffleId, masterLocations, slaveLocations)

    case CommitFiles(shuffleKey, masterIds, slaveIds) =>
      logInfo(s"receive CommitFiles request, $shuffleKey, " +
        s"master files ${masterIds.mkString(",")} slave files ${slaveIds.mkString(",")}")
      val commitFilesTimeMs = Utils.timeIt({
        handleCommitFiles(context, shuffleKey, masterIds, slaveIds)
      })
      logInfo(s"Done processed CommitFiles request with shuffleKey:$shuffleKey, in " +
        s"${commitFilesTimeMs}ms")

    case GetWorkerInfos =>
      logInfo("received GetWorkerInfos request")
      handleGetWorkerInfos(context)

    case ThreadDump =>
      logInfo("receive ThreadDump request")
      handleThreadDump(context)

    case Destroy(shuffleKey, masterLocations, slaveLocations) =>
      logInfo("receive Destroy request")
      handleDestroy(context, shuffleKey, masterLocations, slaveLocations)
  }

  private def handleReserveBuffers(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      masterLocations: util.List[PartitionLocation],
      slaveLocations: util.List[PartitionLocation]): Unit = {
    val masterPartitions = new util.ArrayList[PartitionLocation]()
    for (ind <- 0 until masterLocations.size()) {
      val location = masterLocations.get(ind)
      val writer = localStorageManager.createWriter(applicationId, shuffleId, location)
      masterPartitions.add(new WorkingPartition(location, writer))
    }
    if (masterPartitions.size() < masterLocations.size()) {
      logInfo("not all master partition satisfied, destroy writers")
      masterPartitions.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      context.reply(ReserveBuffersResponse(StatusCode.ReserveBufferFailed))
      return
    }

    val slavePartitions = new util.ArrayList[PartitionLocation]()
    for (ind <- 0 until slaveLocations.size()) {
      val location = slaveLocations.get(ind)
      val writer = localStorageManager.createWriter(applicationId, shuffleId, location)
      slavePartitions.add(new WorkingPartition(slaveLocations.get(ind), writer))
    }
    if (slavePartitions.size() < slaveLocations.size()) {
      logError("not all slave partition satisfied, destroy writers")
      masterPartitions.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      slavePartitions.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      context.reply(ReserveBuffersResponse(StatusCode.ReserveBufferFailed))
      return
    }

    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)

    // reserve success, update status
    workerInfo.addMasterPartitions(shuffleKey, masterPartitions)
    workerInfo.addSlavePartitions(shuffleKey, slavePartitions)

    logDebug(s"reserve buffer succeed!, $shuffleKey")
    logDebug("masters========")
    masterLocations.foreach(loc => logDebug(loc + ", peer " + loc.getPeer))
    logDebug("slaves========")
    slaveLocations.foreach(loc => logDebug(loc + ", peer " + loc.getPeer))

    context.reply(ReserveBuffersResponse(StatusCode.Success))
  }

  private def handleCommitFiles(
      context: RpcCallContext,
      shuffleKey: String,
      masterIds: util.List[String],
      slaveIds: util.List[String]): Unit = {
    // return null if shuffleKey does not exist
    if (!workerInfo.containsShuffle(shuffleKey)) {
      logError(s"shuffle $shuffleKey doesn't exist!")
      context.reply(CommitFilesResponse(
        StatusCode.ShuffleNotRegistered, null, null, masterIds, slaveIds))
      return
    }

    val committedMasterIds = new util.ArrayList[String]()
    val committedSlaveIds = new util.ArrayList[String]()
    val failedMasterIds = new util.ArrayList[String]()
    val failedSlaveIds = new util.ArrayList[String]()

    val futures = new util.ArrayList[Future[_]]()

    if (masterIds != null) {
      masterIds.foreach { id =>
        val target = workerInfo.getMasterLocation(shuffleKey, id)
        if (target != null) {
          val future = commitThreadPool.submit(new Runnable {
            override def run(): Unit = {
              try {
                val bytes = target.asInstanceOf[WorkingPartition].getFileWriter.close()
                if (bytes > 0L) {
                  committedMasterIds.synchronized {
                    committedMasterIds.add(id)
                  }
                }
              } catch {
                case _: IOException =>
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
                  committedSlaveIds.synchronized {
                    committedSlaveIds.add(id)
                  }
                }
              } catch {
                case _: IOException =>
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
      logInfo("CommitFile success!")
      context.reply(CommitFilesResponse(
        StatusCode.Success, committedMasterIds, committedSlaveIds, null, null))
    } else {
      logError("CommitFiles failed!")
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
      logWarning(s"[handleDestroy] shuffle $shuffleKey not registered!")
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
      logInfo("finished handle destroy")
      context.reply(DestroyResponse(StatusCode.Success, null, null))
    } else {
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

  override def handlePushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushData.shuffleKey
    val mode = PartitionLocation.getMode(pushData.mode)
    val body = pushData.body.asInstanceOf[NettyManagedBuffer].getBuf

    // find FileWriter responsible for the data
    val location = if (mode == PartitionLocation.Mode.Master) {
      workerInfo.getMasterLocation(shuffleKey, pushData.partitionUniqueId)
    } else {
      workerInfo.getSlaveLocation(shuffleKey, pushData.partitionUniqueId)
    }
    if (location == null) {
      val msg = s"Partition Location not found!, ${pushData.partitionUniqueId}, $mode $shuffleKey"
      logError(msg)
      callback.onFailure(new IOException(msg))
      return
    }
    val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
    fileWriter.incrementPendingWrites()

    val isMaster = mode == PartitionLocation.Mode.Master
    // for master, send data to slave
    if (EssConf.essReplicate(conf) && isMaster) {
      try {
        val peer = location.getPeer
        val client = dataClientFactory.createClient(
          peer.getHost, peer.getPort, location.getReduceId)
        val newPushData = new PushData(PartitionLocation.Mode.Slave.mode(), shuffleKey,
          pushData.partitionUniqueId, pushData.body)
        pushData.body().retain()
        client.pushData(newPushData, callback)
      } catch {
        case e: Exception =>
          callback.onFailure(e)
      }
    } else {
      callback.onSuccess(ByteBuffer.wrap(new Array[Byte](0)))
    }

    try {
      fileWriter.write(body)
    } catch {
      case e: Exception =>
        val msg = "append data failed!"
        logError(s"msg ${e.getMessage}")
    }
  }

  override def handleOpenStream(shuffleKey: String, fileName: String): DataHandler.FileInfo = {
    // find FileWriter responsible for the data
    val fileWriter = localStorageManager.getWriter(shuffleKey, fileName)
    if (fileWriter eq null) {
      val msg = s"File not found! $shuffleKey, $fileName"
      logError(msg)
      return null
    }
    new DataHandler.FileInfo(
      fileWriter.getFile, fileWriter.getChunkOffsets, fileWriter.getFileLength)
  }

  private def registerWithMaster() {
    logInfo("Trying to register with master")
    var res = masterEndpoint.askSync[RegisterWorkerResponse](
      RegisterWorker(host, port, workerInfo.numSlots, self)
    )
    while (!res.success) {
      logInfo("register worker failed!")
      Thread.sleep(1000)
      logInfo("Trying to re-register with master")
      res = masterEndpoint.askSync[RegisterWorkerResponse](
        RegisterWorker(host, port, workerInfo.numSlots, self)
      )
    }
    logInfo("Registered worker successfully")
  }

  private def reRegisterWithMaster(): Unit = {
    logInfo("Trying to re-register worker!")
    var res = masterEndpoint.askSync[ReregisterWorkerResponse](
      ReregisterWorker(host, port, workerInfo.numSlots, self)
    )
    while (!res.success) {
      Thread.sleep(1000)
      logInfo("Trying to re-register worker!")
      res = masterEndpoint.askSync[ReregisterWorkerResponse](
        ReregisterWorker(host, port, workerInfo.numSlots, self)
      )
    }
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
            logError("cleanup failed", e)
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
    }

    localStorageManager.cleanup(expiredShuffleKeys)
  }
}

private[deploy] object Worker extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new EssConf
    val workerArgs = new WorkerArguments(args, conf)

    val rpcEnv = RpcEnv.create(RpcNameConstants.WORKER_SYS,
      workerArgs.host,
      workerArgs.port,
      conf)

    val masterAddresses = RpcAddress.fromEssURL(workerArgs.master)
    rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP,
      new Worker(rpcEnv, masterAddresses, RpcNameConstants.WORKER_EP, conf))
    rpcEnv.awaitTermination()
  }
}
