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

private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    memory: Long, // In Byte format
    masterRpcAddress: RpcAddress,
    endpointName: String,
    val conf: EssConf) extends RpcEndpoint with Logging {

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  // master endpoint
  val masterEndpoint: RpcEndpointRef =
    rpcEnv.setupEndpointRef(masterRpcAddress, RpcNameConstants.MASTER_EP)

  Utils.checkHost(host)
  assert (port > 0)

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
    if (masterRpcAddress == remoteAddress)  {
      logInfo(s"Master $remoteAddress Disassociated !")
      reRegisterWithMaster()
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case SendHeartbeat =>
      masterEndpoint.send(Heartbeat(host, port))
  }

  // TODO
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ReserveBuffers(shuffleKey, masterLocations, slaveLocations) =>
      handleReserveBuffers(context, shuffleKey, masterLocations, slaveLocations)
    case ClearBuffers(shuffleKey, masterLocs, slaveLocs) =>
      handleClearBuffers(context, shuffleKey, masterLocs, slaveLocs)
    case CommitFiles(shuffleKey, commitLocations, mode) =>
      handleCommitFiles(context, shuffleKey, commitLocations, mode)
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
                new DoubleChunk(ch1, ch2, memoryPool, masterLocations.get(ind).getUUID)))
          }
        }
      }
    })
    if (masterDoubleChunks.size() < masterLocations.size()) {
      logInfo("not all master partition satisfied, return chunks")
      masterDoubleChunks.foreach(entry =>
        entry.asInstanceOf[PartitionLocationWithDoubleChunks].doubleChunk.returnChunks())
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
                new DoubleChunk(ch1, ch2, memoryPool, slaveLocations.get(ind).getUUID)
              )
            )
          }
        }
      }
    })
    if (slaveDoubleChunks.size() < slaveDoubleChunks.size()) {
      logInfo("not all slave partition satisfied, return chunks")
      masterDoubleChunks.foreach(entry =>
        entry.asInstanceOf[PartitionLocationWithDoubleChunks].doubleChunk.returnChunks())
      slaveDoubleChunks.foreach(entry =>
        entry.asInstanceOf[PartitionLocationWithDoubleChunks].doubleChunk.returnChunks())
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

  private def handleClearBuffers(context: RpcCallContext,
    shuffleKey: String,
    masterLocs: util.List[PartitionLocation],
    slaveLocs: util.List[PartitionLocation]): Unit = {
    // clear master partition chunks
    if (!workerInfo.masterPartitionLocations.containsKey(shuffleKey)) {
      logError(s"shuffle ${shuffleKey} doesn't exist!")
      context.reply(ClearBuffersResponse(false))
      return
    }
    val masterLocations = workerInfo.masterPartitionLocations.get(shuffleKey)
    val slaveLocations = workerInfo.slavePartitionLocations.get(shuffleKey)
    if (masterLocs != null) {
      masterLocations.synchronized {
        masterLocs.foreach(loc => {
          val removed = masterLocations.remove(loc)
          if (removed != null) {
            removed.asInstanceOf[PartitionLocationWithDoubleChunks].doubleChunk.returnChunks()
          }
        })
      }
    }
    // clear slave partition chunks
    if (slaveLocs != null) {
      slaveLocations.synchronized {
        slaveLocs.foreach(loc => {
          val removed = slaveLocations.remove(loc)
          if (removed != null) {
            removed.asInstanceOf[PartitionLocationWithDoubleChunks].doubleChunk.returnChunks()
          }
        })
      }
    }
    // reply
    context.reply(ClearBuffersResponse(true))
  }

  private def handleCommitFiles(context: RpcCallContext,
    shuffleKey: String,
    commitLocations: util.List[PartitionLocation],
    mode: PartitionLocation.Mode): Unit = {
    // return null if shuffleKey does not exist
    if (mode == PartitionLocation.Mode.Master) {
      if (!workerInfo.masterPartitionLocations.containsKey(shuffleKey)) {
        logError(s"shuffle ${shuffleKey} doesn't exist!")
        context.reply(CommitFilesResponse(null))
        return
      }
    } else {
      if (!workerInfo.slavePartitionLocations.containsKey(shuffleKey)) {
        logError(s"shuffle ${shuffleKey} doesn't exist!")
        context.reply(CommitFilesResponse(null))
        return
      }
    }

    val committedLocations = new util.ArrayList[PartitionLocation]()
    val locations = mode match {
      case PartitionLocation.Mode.Master =>
        workerInfo.masterPartitionLocations.get(shuffleKey)
      case PartitionLocation.Mode.Slave =>
        workerInfo.slavePartitionLocations.get(shuffleKey)
    }

    if (commitLocations != null) {
      locations.synchronized {
        locations.foreach(loc => {
          val target = locations.get(loc._1)
          if (target != null) {
            if (target.asInstanceOf[PartitionLocationWithDoubleChunks].doubleChunk.flush()) {
              target.asInstanceOf[PartitionLocationWithDoubleChunks].doubleChunk.returnChunks()
              committedLocations.add(target)
            } else {
              target.asInstanceOf[PartitionLocationWithDoubleChunks].doubleChunk.returnChunks()
            }
          }
        })
      }
    }

    // reply
    context.reply(CommitFilesResponse(committedLocations))
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
}

private[deploy] object Worker extends Logging {
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
