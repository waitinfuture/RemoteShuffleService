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
    memory: Int, // In Byte format
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

  // Status
  private var connectionAttemptCount = 0

  // Memory Pool
  private val MemoryPoolCapacity = conf.getSizeAsBytes("ess.memoryPool.capacity", "1G")
  private val ChunkSize = conf.getSizeAsBytes("ess.partition.memory", "32m")
  private val memoryPool = new MemoryPool(MemoryPoolCapacity, ChunkSize)
  private val masterPartitionChunks = new util.HashMap[String, (PartitionLocation, DoubleChunk)]()
  private val slavePartitionChunks = new util.HashMap[String, DoubleChunk]()

  // Threads
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

  // Configs
  private val HEARTBEAT_MILLIS = conf.getLong("jindo.worker.timeout", 60) * 1000 / 4

  // Structs
  var memoryUsed = 0
  def memoryFree: Int = memory - memoryUsed

  override def onStart(): Unit = {
    logInfo("Starting Worker %s:%d with %s RAM".format(
      host, port, Utils.megabytesToString(memory)))
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
      masterEndpoint.ask(Heartbeat(host, port))
  }

  // TODO
  override def receiveAndReply(context: _root_.com.aliyun.emr.jss.common.rpc.RpcCallContext): _root_.scala.PartialFunction[Any, Unit] = {
    case ReserveBuffers(masterLocations, slaveLocations) =>
      handleReserveBuffers(context, masterLocations, slaveLocations)
    case ClearBuffers(masterLocationIds, slaveLocationIds) =>
      handleClearBuffers(context, masterLocationIds, slaveLocationIds)
  }

  private def handleReserveBuffers(context: RpcCallContext,
    masterLocations: util.List[PartitionLocation],
    slaveLocations: util.List[PartitionLocation]): Unit = {
    val masterDoubleChunks = new util.HashMap[String, (PartitionLocation, DoubleChunk)]()
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
            masterDoubleChunks.put(masterLocations.get(ind).getUUID,
              (masterLocations.get(ind),
                new DoubleChunk(ch1, ch2, memoryPool, masterLocations.get(ind).getUUID)))
          }
        }
      }
    })
    if (masterDoubleChunks.size() < masterLocations.size()) {
      logInfo("not all master partition satisfied, return chunks")
      masterDoubleChunks.foreach(entry => entry._2._2.returnChunks())
      context.reply(ReserveBuffersResponse(false))
      return
    }
    val slaveDoubleChunks = new util.HashMap[String, DoubleChunk]()
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
            slaveDoubleChunks.put(slaveLocations.get(ind).getUUID,
              new DoubleChunk(ch1, ch2, memoryPool, slaveLocations.get(ind).getUUID))
          }
        }
      }
    })
    if (slaveDoubleChunks.size() < slaveDoubleChunks.size()) {
      logInfo("not all slave partition satisfied, return chunks")
      masterDoubleChunks.foreach(entry => entry._2._2.returnChunks())
      slaveDoubleChunks.foreach(entry => entry._2.returnChunks())
      context.reply(ReserveBuffersResponse(false))
      return
    }
    masterPartitionChunks.synchronized {
      masterPartitionChunks.putAll(masterDoubleChunks)
    }
    slavePartitionChunks.synchronized {
      slavePartitionChunks.putAll(slaveDoubleChunks)
    }
    context.reply(ReserveBuffersResponse(true))
  }

  private def handleClearBuffers(context: RpcCallContext,
    masterLocationIds: util.List[String],
    slaveLocationIds: util.List[String]): Unit = {
    // clear master partition chunks
    if (masterLocationIds != null) {
      masterPartitionChunks.synchronized {
        masterLocationIds.foreach(id => {
          val entry = masterPartitionChunks.get(id)
          if (entry != null) {
            entry._2.returnChunks()
          }
          masterPartitionChunks.remove(id)
        })
      }
    }
    // clear slave partition chunks
    if (slaveLocationIds != null) {
      slavePartitionChunks.synchronized {
        slaveLocationIds.foreach(id => {
          val doubleChunk = slavePartitionChunks.get(id)
          if (doubleChunk != null) {
            doubleChunk.returnChunks()
          }
          slavePartitionChunks.remove(id)
        })
      }
    }
    // reply
    context.reply(ClearBuffersResponse(true))
  }

  private def registerWithMaster() {
    logInfo("Trying to register with master")
    var res = masterEndpoint.askSync[RegisterWorkerResponse](
      RegisterWorker(host, port, memory, self)
    )
    while (!res.success) {
      Thread.sleep(1000)
      logInfo("Trying to register with master")
      res = masterEndpoint.askSync[RegisterWorkerResponse](
        RegisterWorker(host, port, memory, self)
      )
    }
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
