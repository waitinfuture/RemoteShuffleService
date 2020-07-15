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
import java.util.concurrent.{Future, TimeUnit}

import scala.collection.JavaConversions._

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.rpc._
import com.aliyun.emr.ess.common.util.{EssPathUtil, ThreadUtils, Utils}
import com.aliyun.emr.ess.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.ess.protocol.message.ControlMessages._
import com.aliyun.emr.ess.protocol.message.DataMessages._
import com.aliyun.emr.ess.protocol.message.StatusCode
import com.aliyun.emr.network.TransportContext
import com.aliyun.emr.network.buffer.NettyManagedBuffer
import com.aliyun.emr.network.client.{RpcResponseCallback, TransportClientBootstrap}
import com.aliyun.emr.network.protocol.PushData
import com.aliyun.emr.network.server.TransportServerBootstrap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

private[deploy] class Worker(
  override val rpcEnv: RpcEnv,
  masterRpcAddress: RpcAddress,
  endpointName: String,
  val conf: EssConf)
  extends RpcEndpoint with PushDataHandler with Logging {

  private val (dataServer, dataClientFactory) = {
    val transportConf = Utils.fromEssConf(conf, "data", conf.getInt("ess.data.io.threads", 0))
    val rpcHandler = new PushDataRpcHandler(transportConf, this)
    val transportContext: TransportContext =
      new TransportContext(transportConf, rpcHandler, true)
    val serverBootstraps: Seq[TransportServerBootstrap] = Nil
    val clientBootstraps: Seq[TransportClientBootstrap] = Nil
    (transportContext.createServer(serverBootstraps),
      transportContext.createClientFactory(clientBootstraps))
  }

  private val host = rpcEnv.address.host
  private val port = dataServer.getPort

  // master endpoint
  private val masterEndpoint: RpcEndpointRef =
    rpcEnv.setupEndpointRef(masterRpcAddress, RpcNameConstants.MASTER_EP)

  // slave endpoints
  private val slaveEndpoints: util.Map[RpcAddress, RpcEndpointRef] =
    new util.HashMap[RpcAddress, RpcEndpointRef]()

  Utils.checkHost(host)
  assert(port > 0)

  // Memory Pool
  private val MemoryPoolCapacity = EssConf.essMemoryPoolCapacity(conf)
  private val ChunkSize = EssConf.essPartitionMemory(conf) // double buffer
  private val memoryPool = new MemoryPool(MemoryPoolCapacity, ChunkSize)

  // worker info
  private val workerInfo = new WorkerInfo(host, port, MemoryPoolCapacity, ChunkSize * 2, self)

  // Threads
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

  // Configs
  private val HEARTBEAT_MILLIS = EssConf.essWorkerTimeoutMs(conf) / 4

  // Structs
  private var memoryUsed = 0

  // shared FileSystem
  private val fs = {
    val hadoopConf = new Configuration
    hadoopConf.set("dfs.checksum.type", "NULL")
    hadoopConf.set("dfs.replication", "2")
    val path = new Path(EssConf.essWorkerBaseDir(conf))
    logInfo(s"path ${path}")
    val _fs = path.getFileSystem(hadoopConf)
    if (!_fs.exists(path)) {
      _fs.mkdirs(path)
    }
    _fs
  }

  // shared ExecutorService for flush
  private val commitThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "Worker-CommitFiles", 32);

  private val flushThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "Worker-Flush", 32);

  private def memoryFree: Long = MemoryPoolCapacity - memoryUsed

  override def onStart(): Unit = {
    logInfo("Starting Worker %s:%d with %s RAM".format(
      host, port, Utils.bytesToString(MemoryPoolCapacity)))
    registerWithMaster()

    // start heartbeat
    forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(SendHeartbeat)
      }
    }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    forwordMessageScheduler.shutdownNow()
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
      masterEndpoint.send(HeartbeatFromWorker(host, port))
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ReserveBuffers(shuffleKey, masterLocations, slaveLocations) =>
      logInfo(s"received ReserveBuffers request, $shuffleKey")
      handleReserveBuffers(context, shuffleKey, masterLocations, slaveLocations)

    case CommitFiles(shuffleKey, commitLocations, mode) =>
      logInfo(s"receive CommitFiles request, $shuffleKey, $mode")
      val commitFilesTimeMs = Utils.timeIt({
        handleCommitFiles(context, shuffleKey, commitLocations, mode)
      })
      logInfo(s"Done processed CommitFiles request with shuffleKey:$shuffleKey,mode:$mode in " +
        s"${commitFilesTimeMs}ms")

    case Destroy(shuffleKey, masterLocations, slaveLocations) =>
      logInfo(s"received Destroy request, $shuffleKey")
      handleDestroy(context, shuffleKey, masterLocations, slaveLocations)
    case ReplicateData(shuffleKey, partitionLocation, working, masterData, slaveData) =>
      logInfo(s"received ReplicateData request, ${shuffleKey}, ${partitionLocation}")
      handleReplicateData(context, shuffleKey, partitionLocation, working, masterData, slaveData)

    case GetWorkerInfos =>
      logInfo("received GetWorkerInfos request")
      handleGetWorkerInfos(context)

    case GetDoubleChunkInfo(shuffleKey, mode, partitionLocation) =>
      logInfo("received GetDoubleChunkInfo request")
      handleGetDoubleChunkInfo(context, shuffleKey, mode, partitionLocation)

    case ThreadDump =>
      logInfo("receive ThreadDump request")
      handleThreadDump(context)
  }

  private def handleReserveBuffers(context: RpcCallContext, shuffleKey: String,
    masterLocations: util.List[PartitionLocation],
    slaveLocations: util.List[PartitionLocation]): Unit = {
    val masterDoubleChunks = new util.ArrayList[PartitionLocation]()
    // allocate chunks for master
    val masterChunks = memoryPool.synchronized {
      memoryPool.allocateChunks(masterLocations.size() * 2)
    }
    // construct double chunks
    if (masterChunks.size != masterLocations.size() * 2) {
      logError("Allocate chunks failed!")
      memoryPool.synchronized {
        memoryPool.returnChunks(masterChunks)
      }
    } else {
      for (ind <- 0 until masterLocations.size()) {
        val ch1 = masterChunks(ind * 2)
        val ch2 = masterChunks(ind * 2 + 1)
        val dc = new DoubleChunk(ch1, ch2, memoryPool,
          EssPathUtil.GetPartitionPath(conf,
            shuffleKey.split("-").dropRight(1).mkString("-"),
            shuffleKey.split("-").last.toInt,
            masterLocations.get(ind).getReduceId,
            masterLocations.get(ind).getEpoch,
            PartitionLocation.Mode.Master),
          fs,
          flushThreadPool
        )
        masterDoubleChunks.add(
          new PartitionLocationWithDoubleChunks(
            masterLocations.get(ind),
            dc
          )
        )
      }
    }
    if (masterDoubleChunks.size() < masterLocations.size()) {
      logInfo("not all master partition satisfied, return chunks")
      masterDoubleChunks.foreach(entry =>
        entry.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk.returnChunks())
      context.reply(ReserveBuffersResponse(StatusCode.ReserveBufferFailed))
      return
    }
    val slaveDoubleChunks = new util.ArrayList[PartitionLocation]()
    // allocate chunks for slave
    val slaveChunks = memoryPool.synchronized {
      memoryPool.allocateChunks(slaveLocations.size() * 2)
    }
    // construct double chunks
    if (slaveChunks.size != slaveLocations.size() * 2) {
      logError("Allocate chunks failed!")
      memoryPool.synchronized {
        memoryPool.returnChunks(slaveChunks)
      }
    } else {
      for (ind <- 0 until slaveLocations.size()) {
        val ch1 = slaveChunks(ind * 2)
        val ch2 = slaveChunks(ind * 2 + 1)
        val dc = new DoubleChunk(ch1, ch2, memoryPool,
          EssPathUtil.GetPartitionPath(conf,
            shuffleKey.split("-").dropRight(1).mkString("-"),
            shuffleKey.split("-").last.toInt,
            slaveLocations.get(ind).getReduceId,
            slaveLocations.get(ind).getEpoch,
            PartitionLocation.Mode.Slave),
          fs,
          flushThreadPool
        )
        slaveDoubleChunks.add(
          new PartitionLocationWithDoubleChunks(
            slaveLocations.get(ind),
            dc
          )
        )
      }
    }
    if (slaveDoubleChunks.size() < slaveLocations.size()) {
      logError("not all slave partition satisfied, return chunks")
      masterDoubleChunks.foreach(entry =>
        entry.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk.returnChunks())
      slaveDoubleChunks.foreach(entry =>
        entry.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk.returnChunks())
      context.reply(ReserveBuffersResponse(StatusCode.ReserveBufferFailed))
      return
    }
    // reserve success, update status
    workerInfo.addMasterPartition(shuffleKey, masterDoubleChunks)
    workerInfo.addSlavePartition(shuffleKey, slaveDoubleChunks)

    logDebug(s"reserve buffer succeed!, ${shuffleKey}")
    logDebug("masters========")
    masterLocations.foreach(loc => {
      logDebug(loc + ", peer " + loc.getPeer)
    })
    logDebug("slaves========")
    slaveLocations.foreach(loc => {
      logDebug(loc + ", peer " + loc.getPeer)
    })

    context.reply(ReserveBuffersResponse(StatusCode.Success))
  }

  private def handleCommitFiles(context: RpcCallContext,
    shuffleKey: String,
    commitLocationIds: util.List[String],
    mode: PartitionLocation.Mode): Unit = {
    // return null if shuffleKey does not exist
    if (mode == PartitionLocation.Mode.Master) {
      if (!workerInfo.containsShuffleMaster(shuffleKey)) {
        logError(s"shuffle ${shuffleKey} doesn't exist!")
        context.reply(CommitFilesResponse(StatusCode.ShuffleNotRegistered, commitLocationIds, null))
        return
      }
    } else if (mode == PartitionLocation.Mode.Slave) {
      if (!workerInfo.containsShuffleSlave(shuffleKey)) {
        logError(s"shuffle ${shuffleKey} doesn't exist!")
        context.reply(CommitFilesResponse(StatusCode.ShuffleNotRegistered, commitLocationIds, null))
        return
      }
    }

    val failedLocations = new util.ArrayList[String]()
    val committedLocations = new util.ArrayList[String]()

    val futures = new util.ArrayList[Future[_]]()
    if (commitLocationIds != null) {
      commitLocationIds.foreach(id => {
        val target = if (mode == PartitionLocation.Mode.Master) {
          workerInfo.getMasterLocation(shuffleKey, id)
        } else {
          workerInfo.getSlaveLocation(shuffleKey, id)
        }

        if (target != null) {
          val future = commitThreadPool.submit(new Runnable {
            override def run(): Unit = {
              val res = target.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk.flush()
              if (res == -1) {
                failedLocations.synchronized {
                  failedLocations.add(id)
                }
              } else if (res == 0) {
                committedLocations.synchronized {
                  committedLocations.add(id)
                }
              }
            }
          })
          futures.add(future)
        }
      })
      futures.foreach(f => f.get(30, TimeUnit.SECONDS))
    }

    // reply
    if (failedLocations.isEmpty) {
      logInfo("CommitFile success!")
      context.reply(CommitFilesResponse(StatusCode.Success, null, committedLocations))
    } else {
      logError("CommitFiles failed!")
      context.reply(CommitFilesResponse(StatusCode.PartialSuccess, failedLocations, committedLocations))
    }
  }

  private def handleDestroy(context: RpcCallContext,
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
      masterLocations.foreach(loc => {
        val allocatedLoc = workerInfo.getMasterLocation(shuffleKey, loc)
        if (allocatedLoc == null) {
          failedMasters.add(loc)
        } else {
          allocatedLoc.asInstanceOf[PartitionLocationWithDoubleChunks]
            .getDoubleChunk.returnChunks()
        }
      })
      // remove master locations from workerinfo
      workerInfo.removeMasterPartition(shuffleKey, masterLocations)
    }
    // destroy slave locations
    if (slaveLocations != null && !slaveLocations.isEmpty) {
      slaveLocations.foreach(loc => {
        val allocatedLoc = workerInfo.getSlaveLocation(shuffleKey, loc)
        if (allocatedLoc == null) {
          failedSlaves.add(loc)
        } else {
          allocatedLoc.asInstanceOf[PartitionLocationWithDoubleChunks]
            .getDoubleChunk.returnChunks()
        }
      })
      // remove slave locations from worker info
      workerInfo.removeSlavePartition(shuffleKey, slaveLocations)
    }
    // reply
    if (failedMasters.isEmpty && failedSlaves.isEmpty) {
      logInfo("finished handle destroy")
      context.reply(DestroyResponse(StatusCode.Success, null, null))
    } else {
      context.reply(DestroyResponse(StatusCode.PartialSuccess, failedMasters, failedSlaves))
    }
  }

  private def handleReplicateData(context: RpcCallContext,
    shuffleKey: String, partitionLocation: PartitionLocation,
    working: Int, masterData: Array[Byte], slaveData: Array[Byte]): Unit = {
    if (!workerInfo.containsShuffleSlave(shuffleKey)) {
      val msg = s"shuffleKey ${
        shuffleKey
      } Not Found!"
      logError(msg)
      context.reply(ReplicateDataResponse(StatusCode.ShuffleNotRegistered, msg))
      return
    }
    val partition = workerInfo.getSlaveLocation(shuffleKey, partitionLocation.getUniqueId)
    if (partition == null) {
      val msg = s"partition ${
        partitionLocation
      } not found!"
      logError(msg)
      context.reply(ReplicateDataResponse(StatusCode.PartitionNotFound, msg))
      return
    }
    val doubleChunk = partition.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk
    doubleChunk.synchronized {
      doubleChunk.initWithData(working, masterData, slaveData)
    }
    context.reply(ReplicateDataResponse(StatusCode.Success, null))
  }

  private def handleGetWorkerInfos(context: RpcCallContext): Unit = {
    val list = new util.ArrayList[WorkerInfo]()
    list.add(workerInfo)
    context.reply(GetWorkerInfosResponse(StatusCode.Success, list))
  }

  private def handleGetDoubleChunkInfo(context: RpcCallContext,
    shuffleKey: String, mode: PartitionLocation.Mode,
    loc: PartitionLocation): Unit = {
    val location = if (mode == PartitionLocation.Mode.Master) {
      workerInfo.getMasterLocation(shuffleKey, loc.getUniqueId)
    } else {
      workerInfo.getSlaveLocation(shuffleKey, loc.getUniqueId)
    }
    if (location == null) {
      context.reply(GetDoubleChunkInfoResponse(false, -1, -1, null, -1, null))
      return
    }
    val doubleChunk = location.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk
    context.reply(GetDoubleChunkInfoResponse(
      true,
      doubleChunk.working,
      doubleChunk.chunks(doubleChunk.working).remaining(),
      doubleChunk.getMasterData,
      doubleChunk.chunks((doubleChunk.working + 1) % 2).remaining(),
      doubleChunk.getSlaveData
    ))
  }

  override def handlePushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushData.shuffleKey
    val mode = PartitionLocation.getMode(pushData.mode)
    val body = pushData.body.asInstanceOf[NettyManagedBuffer].getBuf

    // find DoubleChunk responsible for the data
    val location = if (mode == PartitionLocation.Mode.Master) {
      workerInfo.getMasterLocation(shuffleKey, pushData.partitionUniqueId)
    } else {
      workerInfo.getSlaveLocation(shuffleKey, pushData.partitionUniqueId)
    }
    if (location == null) {
      val msg = s"Partition Location not found!, ${
        pushData.partitionUniqueId
      }, ${
        mode
      } ${
        shuffleKey
      }"
      logError(msg)
      callback.onFailure(new IOException(msg))
      return
    }
    val doubleChunk = location.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk

    val isMaster = mode == PartitionLocation.Mode.Master

    // append data
    val epoch = try {
      if (isMaster) doubleChunk.append(body) else doubleChunk.append(body, pushData.epoch)
    } catch {
      case e: Exception =>
        val msg = "append data failed!"
        logError(s"msg ${
          e.getMessage
        }")
        callback.onFailure(new IOException(msg, e))
        return
    }

    // for master, send data to slave
    if (EssConf.essReplicate(conf) && isMaster) {
      try {
        val peer = location.getPeer
        val client = dataClientFactory.createClient(peer.getHost, peer.getPort)
        val newPushData = new PushData(epoch, PartitionLocation.Mode.Slave.mode(),
          shuffleKey, pushData.partitionUniqueId, pushData.body)
        pushData.body().retain()
        client.pushData(newPushData, callback)
      } catch {
        case e: Exception => {
          callback.onFailure(e)
        }
      }
    } else {
      callback.onSuccess(ByteBuffer.wrap(new Array[Byte](0)))
    }
  }

  private def handleThreadDump(context: RpcCallContext): Unit = {
    val threadDump = Utils.getThreadDump()
    context.reply(ThreadDumpResponse(threadDump))
  }

  private def registerWithMaster() {
    logInfo("Trying to register with master")
    var res = masterEndpoint.askSync[RegisterWorkerResponse](
      RegisterWorker(host, port, MemoryPoolCapacity, self)
    )
    while (!res.success) {
      logInfo("register worker failed!")
      Thread.sleep(1000)
      logInfo("Trying to re-register with master")
      res = masterEndpoint.askSync[RegisterWorkerResponse](
        RegisterWorker(host, port, MemoryPoolCapacity, self)
      )
    }
    logInfo("Registered worker successfully")
  }

  private def reRegisterWithMaster(): Unit = {
    logInfo("Trying to reregister worker!")
    var res = masterEndpoint.askSync[ReregisterWorkerResponse](
      ReregisterWorker(host, port, MemoryPoolCapacity, self)
    )
    while (!res.success) {
      Thread.sleep(1000)
      logInfo("Trying to reregister worker!")
      res = masterEndpoint.askSync[ReregisterWorkerResponse](
        ReregisterWorker(host, port, MemoryPoolCapacity, self)
      )
    }
  }

  private def getOrCreateEndpoint(host: String, port: Int): RpcEndpointRef = {
    val slaveAddress = RpcAddress(host, port)
    if (!slaveEndpoints.contains(slaveAddress)) {
      slaveEndpoints.synchronized {
        if (!slaveEndpoints.contains(slaveAddress)) {
          var slaveEndpoint: RpcEndpointRef = null
          try {
            slaveEndpoint = rpcEnv.setupEndpointRef(slaveAddress, RpcNameConstants.WORKER_EP)
          } catch {
            case e: Exception => {
              println(e.getStackTrace)
              e.printStackTrace()
            }
          }
          slaveEndpoints.putIfAbsent(slaveAddress, slaveEndpoint)
        }
      }
    }
    slaveEndpoints.get(slaveAddress)
  }
}

private[deploy] object Worker
  extends Logging {
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
