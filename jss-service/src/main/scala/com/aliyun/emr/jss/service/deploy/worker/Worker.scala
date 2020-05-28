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

package com.aliyun.emr.jss.service.deploy.worker

import java.util
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import com.aliyun.emr.jss.common.EssConf
import com.aliyun.emr.jss.common.internal.Logging
import com.aliyun.emr.jss.common.rpc._
import com.aliyun.emr.jss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ControlMessages._
import com.aliyun.emr.jss.protocol.message.DataMessages._
import com.aliyun.emr.jss.protocol.message.ReturnCode
import com.aliyun.emr.jss.service.deploy.common.EssPathUtil
import io.netty.buffer.ByteBuf

private[deploy] class Worker(
  override val rpcEnv: RpcEnv,
  memory: Long, // In Byte format
  masterRpcAddress: RpcAddress,
  endpointName: String,
  val conf: EssConf)
  extends RpcEndpoint with Logging {

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  // master endpoint
  val masterEndpoint: RpcEndpointRef =
    rpcEnv.setupEndpointRef(masterRpcAddress, RpcNameConstants.MASTER_EP)

  // slave endpoints
  val slaveEndpoints: util.Map[RpcAddress, RpcEndpointRef] =
    new util.HashMap[RpcAddress, RpcEndpointRef]()

  Utils.checkHost(host)
  assert(port > 0)

  // Memory Pool
  private val MemoryPoolCapacity = conf.getSizeAsBytes("ess.memoryPool.capacity", "1G")
  private val ChunkSize = conf.getSizeAsBytes("ess.partition.memory", "32m")
  private val memoryPool = new MemoryPool(MemoryPoolCapacity, ChunkSize)

  // worker info
  private val workerInfo = new WorkerInfo(host, port, memory, ChunkSize, self)

  // Threads
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

  // Configs
  private val HEARTBEAT_MILLIS = conf.getLong("jindo.worker.timeout", 60) * 1000 / 4

  // Structs
  var memoryUsed = 0

  def memoryFree: Long = memory - memoryUsed

  override def onStart(): Unit = {
    logInfo("Starting Worker %s:%d with %s RAM".format(
      host, port, Utils.bytesToString(memory)))
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
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (masterRpcAddress == remoteAddress) {
      logInfo(s"Master $remoteAddress Disassociated !")
      reRegisterWithMaster()
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case SendHeartbeat =>
      masterEndpoint.send(Heartbeat(host, port))
    case SlaveLost(shuffleKey, masterLocation, slaveLocation) =>
      logInfo(s"received SlaveLost ${shuffleKey}, ${masterLocation}, ${slaveLocation}")
      handleSlaveLost(null, shuffleKey, masterLocation, slaveLocation)
  }

  // TODO
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ReserveBuffers(shuffleKey, masterLocations, slaveLocations) =>
      logInfo("receive ReserveBuffers request," +
        s"${shuffleKey}, ${masterLocations}, ${slaveLocations}")
      handleReserveBuffers(context, shuffleKey, masterLocations, slaveLocations)
    case CommitFiles(shuffleKey, commitLocations, mode) =>
      logInfo(s"receive CommitFiles request, ${shuffleKey}, ${mode}")
      handleCommitFiles(context, shuffleKey, commitLocations, mode)
    case Destroy(shuffleKey, masterLocations, slaveLocations) =>
      logInfo(s"receive Destroy request, ${shuffleKey} ${masterLocations} ${slaveLocations}")
      handleDestroy(context, shuffleKey, masterLocations, slaveLocations)
    case SendData(shuffleKey, partitionLocation, mode, flush, data) =>
      logInfo(s"Worker ${host}:${port} receive SendData request, ${mode}, ${shuffleKey}, ${partitionLocation.getUUID}")
      handleSendData(context, shuffleKey, partitionLocation, mode, flush, data)
    case ReplicateData(shuffleKey, partitionLocation, working, masterData, slaveData) =>
      logInfo(s"received ReplicateData request, ${shuffleKey}, ${partitionLocation}")
      handleReplicateData(context, shuffleKey, partitionLocation, working, masterData, slaveData)
    case GetWorkerInfos =>
      logInfo("received GetWorkerInfos request")
      handleGetWorkerInfos(context)
    case GetDoubleChunkInfo(shuffleKey, mode, partitionLocation) =>
      logInfo("received GetDoubleChunkInfo request")
      handleGetDoubleChunkInfo(context, shuffleKey, mode, partitionLocation)
    case SlaveLost(shuffleKey, masterLocation, slaveLocation) =>
      logInfo(s"received SlaveLost ${shuffleKey}, ${masterLocation}, ${slaveLocation}")
      handleSlaveLost(context, shuffleKey, masterLocation, slaveLocation)
  }

  private def handleReserveBuffers(context: RpcCallContext, shuffleKey: String,
    masterLocations: util.List[PartitionLocation],
    slaveLocations: util.List[PartitionLocation]): Unit = {
    val masterDoubleChunks = new util.ArrayList[PartitionLocation]()
    breakable({
      for (ind <- 0 until masterLocations.size()) {
        val ch1 = memoryPool.allocateChunk()
        if (ch1 == null) {
          break()
        } else {
          val ch2 = memoryPool.allocateChunk()
          if (ch2 == null) {
            memoryPool.returnChunk(ch1)
            break()
          } else {
            masterDoubleChunks.add(
              new PartitionLocationWithDoubleChunks(
                masterLocations.get(ind),
                new DoubleChunk(ch1, ch2, memoryPool,
                  EssPathUtil.GetPartitionPath(conf,
                    shuffleKey.split("-").dropRight(1).mkString("-"),
                    shuffleKey.split("-").last.toInt,
                    masterLocations.get(ind).getUUID)
                )
              )
            )
          }
        }
      }
    })
    if (masterDoubleChunks.size() < masterLocations.size()) {
      logInfo("not all master partition satisfied, return chunks")
      masterDoubleChunks.foreach(entry =>
        entry.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk.returnChunks())
      context.reply(ReserveBuffersResponse(false))
      return
    }
    val slaveDoubleChunks = new util.ArrayList[PartitionLocation]()
    breakable({
      for (ind <- 0 until slaveLocations.size()) {
        val ch1 = memoryPool.allocateChunk()
        if (ch1 == null) {
          break()
        } else {
          val ch2 = memoryPool.allocateChunk()
          if (ch2 == null) {
            break()
          } else {
            slaveDoubleChunks.add(
              new PartitionLocationWithDoubleChunks(
                slaveLocations.get(ind),
                new DoubleChunk(ch1, ch2, memoryPool,
                  EssPathUtil.GetPartitionPath(conf,
                    shuffleKey.split("-").dropRight(1).mkString("-"),
                    shuffleKey.split("-").last.toInt,
                    slaveLocations.get(ind).getUUID)
                )
              )
            )
          }
        }
      }
    })
    if (slaveDoubleChunks.size() < slaveLocations.size()) {
      logInfo("not all slave partition satisfied, return chunks")
      masterDoubleChunks.foreach(entry =>
        entry.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk.returnChunks())
      slaveDoubleChunks.foreach(entry =>
        entry.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk.returnChunks())
      context.reply(ReserveBuffersResponse(false))
      return
    }
    // reserve success, update status
    workerInfo.synchronized {
      workerInfo.addMasterPartition(shuffleKey, masterDoubleChunks)
      workerInfo.addSlavePartition(shuffleKey, slaveDoubleChunks)
    }
    context.reply(ReserveBuffersResponse(true))
  }

  private def handleCommitFiles(context: RpcCallContext,
    shuffleKey: String,
    commitLocations: util.List[PartitionLocation],
    mode: PartitionLocation.Mode): Unit = {
    // return null if shuffleKey does not exist
    if (mode == PartitionLocation.Mode.Master) {
      if (!workerInfo.masterPartitionLocations.containsKey(shuffleKey)) {
        logError(s"shuffle ${shuffleKey} doesn't exist!")
        context.reply(CommitFilesResponse(ReturnCode.ShuffleNotRegistered, commitLocations))
        return
      }
    } else {
      if (!workerInfo.slavePartitionLocations.containsKey(shuffleKey)) {
        logError(s"shuffle ${shuffleKey} doesn't exist!")
        context.reply(CommitFilesResponse(ReturnCode.ShuffleNotRegistered, commitLocations))
        return
      }
    }

    val failedLocations = new util.ArrayList[PartitionLocation]()
    val locations = mode match {
      case PartitionLocation.Mode.Master =>
        workerInfo.masterPartitionLocations.get(shuffleKey)
      case PartitionLocation.Mode.Slave =>
        workerInfo.slavePartitionLocations.get(shuffleKey)
    }

    if (commitLocations != null) {
      locations.synchronized {
        commitLocations.foreach(loc => {
          val target = locations.get(loc)
          if (target != null) {
            if (!target.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk.flush()) {
              failedLocations.add(target)
            }
          }
        })
      }
    }

    // reply
    if (failedLocations.isEmpty) {
      context.reply(CommitFilesResponse(ReturnCode.Success, null))
    } else {
      logInfo("CommitFiles success!")
      context.reply(CommitFilesResponse(ReturnCode.PartialSuccess, failedLocations))
    }
  }

  private def handleDestroy(context: RpcCallContext,
    shuffleKey: String,
    masterLocations: util.List[PartitionLocation],
    slaveLocations: util.List[PartitionLocation]): Unit = {
    // check whether shuffleKey has registered
    if (!workerInfo.masterPartitionLocations.containsKey(shuffleKey) &&
      !workerInfo.slavePartitionLocations.containsKey(shuffleKey)) {
      logError(s"shuffle ${shuffleKey} doesn't exist!")
      context.reply(DestroyResponse(
        ReturnCode.ShuffleNotRegistered, masterLocations, slaveLocations))
      return
    }

    val failedMasters = new util.ArrayList[PartitionLocation]()
    val failedSlaves = new util.ArrayList[PartitionLocation]()

    // destroy master locations
    if (masterLocations != null && !masterLocations.isEmpty) {
      val allocatedMasterLocations = workerInfo.masterPartitionLocations.get(shuffleKey)
      allocatedMasterLocations.synchronized {
        masterLocations.foreach(loc => {
          if (!allocatedMasterLocations.containsKey(loc)) {
            failedMasters.add(loc)
          } else {
            allocatedMasterLocations.get(loc).asInstanceOf[PartitionLocationWithDoubleChunks]
              .getDoubleChunk.returnChunks()
          }
        })
      }
      // remove master locations from workerinfo
      workerInfo.removeMasterPartition(shuffleKey, masterLocations)
    }
    // destroy slave locations
    if (slaveLocations != null && !slaveLocations.isEmpty) {
      val allocatedSlaveLocations = workerInfo.slavePartitionLocations.get(shuffleKey)
      allocatedSlaveLocations.synchronized {
        slaveLocations.foreach(loc => {
          if (!allocatedSlaveLocations.containsKey(loc)) {
            failedSlaves.add(loc)
          } else {
            allocatedSlaveLocations.get(loc).asInstanceOf[PartitionLocationWithDoubleChunks]
              .getDoubleChunk.returnChunks()
          }
        })
      }
      // remove slave locations from worker info
      workerInfo.removeSlavePartition(shuffleKey, slaveLocations)
    }
    // reply
    if (failedMasters.isEmpty && failedSlaves.isEmpty) {
      logInfo("finished handle destroy")
      context.reply(DestroyResponse(ReturnCode.Success, null, null))
    } else {
      context.reply(DestroyResponse(ReturnCode.PartialSuccess, failedMasters, failedSlaves))
    }
  }

  private def handleSendData(
    context: RpcCallContext,
    shuffleKey: String,
    partitionLocation: PartitionLocation,
    mode: PartitionLocation.Mode,
    flush: Boolean,
    //    data: ByteBuf): Unit = {
    data: Array[Byte]): Unit = {
    // find DoubleChunk responsible for the data
    val shufflelocations = if (mode == PartitionLocation.Mode.Master) {
      workerInfo.masterPartitionLocations.get(shuffleKey)
    } else {
      workerInfo.slavePartitionLocations.get(shuffleKey)
    }
    if (shufflelocations == null) {
      val msg = "shuffle shufflelocations not found!"
      logError(msg)
      context.reply(SendDataResponse(false, msg))
      return
    }
    val location = shufflelocations.get(partitionLocation)
    if (location == null) {
      val msg = "Partition Location not found!"
      logError(msg)
      context.reply(SendDataResponse(false, msg))
      return
    }
    val doubleChunk = location.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk

    // append data
    val appended = doubleChunk.append(data, flush)
    if (!appended) {
      val msg = "append data failed!"
      logError(msg)
      context.reply(SendDataResponse(false, msg))
      return
    }

    // for master, send data to slave
    if (mode == PartitionLocation.Mode.Master) {
      logInfo("replicating data to slave")
      val peer = location.getPeer
      val slaveEndpoint = getOrCreateEndpoint(peer.getHost, peer.getPort)
      var res = slaveEndpoint.askSync[SendDataResponse](
        SendData(shuffleKey, peer, PartitionLocation.Mode.Slave, false, data)
      )
      if (!res.success) {
        // retry once
        res = slaveEndpoint.askSync[SendDataResponse](
          SendData(shuffleKey, peer, PartitionLocation.Mode.Slave, false, data)
        )
      }
      if (res.success) {
        context.reply(SendDataResponse(true, null))
      } else {
        val msg = s"send data to slave failed! ${res.msg}"
        logError(msg)
        // send SlaveLost to self
        self.send(SlaveLost(shuffleKey, location, peer))
        context.reply(SendDataResponse(false, msg))
      }
    } else {
      context.reply(SendDataResponse(true, null))
    }
  }

  private def handleReplicateData(context: RpcCallContext,
    shuffleKey: String, partitionLocation: PartitionLocation,
    working: Int, masterData: Array[Byte], slaveData: Array[Byte]): Unit = {
    if (!workerInfo.slavePartitionLocations.contains(shuffleKey)) {
      val msg = s"shuffleKey ${shuffleKey} Not Found!"
      logError(msg)
      context.reply(ReplicateDataResponse(ReturnCode.ShuffleNotRegistered, msg))
      return
    }
    val partition = workerInfo.slavePartitionLocations.get(shuffleKey).get(partitionLocation)
    if (partition == null) {
      val msg = s"partition ${partitionLocation} not found!"
      logError(msg)
      context.reply(ReplicateDataResponse(ReturnCode.PartitionNotFound, msg))
      return
    }
    val doubleChunk = partition.asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk
    doubleChunk.synchronized {
      doubleChunk.initWithData(working, masterData, slaveData)
    }
    context.reply(ReplicateDataResponse(ReturnCode.Success, null))
  }

  private def handleSlaveLost(context: RpcCallContext, shuffleKey: String,
    masterLocation: PartitionLocation, slaveLocation: PartitionLocation): Unit = {
    // send peer to null
    workerInfo.masterPartitionLocations.get(shuffleKey).get(masterLocation).setPeer(null)
    // send SlaveLost to Master to get new Slave
    val res = masterEndpoint.askSync[SlaveLostResponse](
      SlaveLost(shuffleKey, masterLocation, slaveLocation)
    )
    // if Master doesn't know me, remove master location
    if (res.returnCode == ReturnCode.MasterPartitionNotFound) {
      logError("Master doesn't know me, remove myself")
      val master = workerInfo.masterPartitionLocations.get(shuffleKey).get(masterLocation)
          .asInstanceOf[PartitionLocationWithDoubleChunks]
      master.getDoubleChunk.returnChunks()
      workerInfo.synchronized {
        workerInfo.removeMasterPartition(shuffleKey,
          workerInfo.masterPartitionLocations.get(shuffleKey).keySet().toList)
      }
      return
    }
    // if Master offer slave failed, flush and destroy self
    if (res.returnCode != ReturnCode.Success) {
      logError(s"Master process SlaveLost failed! ${res.returnCode}," +
        " flush and destroy master location")
      // remove master location
      val loc = workerInfo.masterPartitionLocations.get(shuffleKey).get(masterLocation)
          .asInstanceOf[PartitionLocationWithDoubleChunks]
      workerInfo.synchronized {
        workerInfo.removeMasterPartition(shuffleKey, masterLocation)
      }
      // flush data
      logInfo(s"worker ${workerInfo} flush data")
      loc.getDoubleChunk.flush()
      // return chunks
      loc.getDoubleChunk.returnChunks()
      // tell master that the partition suicide
      masterEndpoint.askSync[MasterPartitionSuicideResponse](
        MasterPartitionSuicide(shuffleKey, masterLocation)
      )
      return
    }
    // update master location's peer
    val master = workerInfo.masterPartitionLocations.get(shuffleKey).get(masterLocation)
        .asInstanceOf[PartitionLocationWithDoubleChunks]
    master.synchronized {
      master.setPeer(slaveLocation)
    }
    // replicate data
    val slaveEndpoint = getOrCreateEndpoint(slaveLocation.getHost, slaveLocation.getPort)
    val doubleChunk = master.getDoubleChunk
    doubleChunk.synchronized {
      // wait for slave finish flushing before replicate
      while (doubleChunk.slaveState == DoubleChunk.ChunkState.Flushing) {
        Thread.sleep(100)
      }
      // replicate data
      val res = slaveEndpoint.askSync[ReplicateDataResponse](
        ReplicateData(shuffleKey,
          slaveLocation,
          doubleChunk.working,
          doubleChunk.getMasterData,
          doubleChunk.getSlaveData)
      )
      if (res.returnCode != ReturnCode.Success) {
        logError(s"Replicate data failed! ${res.msg}")
        self.send(SlaveLost(shuffleKey, masterLocation, slaveLocation))
      }
    }
    logInfo("handle SlaveLost success")
    if (context != null) {
      context.reply(SlaveLostResponse(ReturnCode.Success, slaveLocation))
    }
  }

  private def handleGetWorkerInfos(context: RpcCallContext): Unit = {
    val list = new util.ArrayList[WorkerInfo]()
    list.add(workerInfo)
    context.reply(GetWorkerInfosResponse(true, list))
  }

  private def handleGetDoubleChunkInfo(context: RpcCallContext,
    shuffleKey: String, mode: PartitionLocation.Mode,
    loc: PartitionLocation): Unit = {
    val location = if (mode == PartitionLocation.Mode.Master) {
      workerInfo.masterPartitionLocations.get(shuffleKey).get(loc)
    } else {
      workerInfo.slavePartitionLocations.get(shuffleKey).get(loc)
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
      doubleChunk.getSlaveData,
      doubleChunk.chunks((doubleChunk.working + 1) % 2).remaining(),
      doubleChunk.getMasterData
    ))
  }

  private def registerWithMaster() {
    logInfo("Trying to register with master")
    var res = masterEndpoint.askSync[RegisterWorkerResponse](
      RegisterWorker(host, port, memory, self)
    )
    while (!res.success) {
      logInfo("register worker failed!")
      Thread.sleep(1000)
      logInfo("Trying to re-register with master")
      res = masterEndpoint.askSync[RegisterWorkerResponse](
        RegisterWorker(host, port, memory, self)
      )
    }
    logInfo("Registered worker successfully")
  }

  private def reRegisterWithMaster(): Unit = {
    logInfo("Trying to reregister worker!")
    var res = masterEndpoint.askSync[ReregisterWorkerResponse](
      ReregisterWorker(host, port, memory, self)
    )
    while (!res.success) {
      Thread.sleep(1000)
      logInfo("Trying to reregister worker!")
      res = masterEndpoint.askSync[ReregisterWorkerResponse](
        ReregisterWorker(host, port, memory, self)
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

    val masterAddresses = RpcAddress.fromJindoURL(workerArgs.master)
    rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP,
      new Worker(rpcEnv, workerArgs.memory,
        masterAddresses, RpcNameConstants.WORKER_EP, conf))
    rpcEnv.awaitTermination()
  }
}
