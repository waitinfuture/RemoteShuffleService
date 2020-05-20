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
import java.util.Date
import java.util.concurrent.{TimeUnit, Future => JFuture, ScheduledFuture => JScheduledFuture}
import scala.util.control.Breaks._
import scala.collection.JavaConversions._

import com.aliyun.emr.jss.common.JindoConf
import com.aliyun.emr.jss.common.internal.Logging
import com.aliyun.emr.jss.common.rpc._
import com.aliyun.emr.jss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.jss.protocol.RpcNameConstants
import com.aliyun.emr.jss.protocol.message.ShuffleMessages.{ClearBuffers, ClearBuffersResponse, RegisterPartition, RegisterPartitionResponse, ReserveBuffers, ReserveBuffersResponse}
import com.aliyun.emr.jss.service.deploy.DeployMessages._

private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    memory: Int, // In MB format
    masterRpcAddress: RpcAddress,
    endpointName: String,
    val conf: JindoConf) extends RpcEndpoint with Logging {

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  Utils.checkHost(host)
  assert (port > 0)

  // Status
  private var registered = false
  private var connected = false
  private var connectionAttemptCount = 0

  // Memory Pool
  private val MemoryPoolCapacity = conf.getLong("ess.memorypool.capacity", 1 * 1024 * 1024 * 1024)
  private val ChunkSize = conf.getLong("ess.chunksize", 32 * 1024 * 1024)
  private val memoryPool = new MemoryPool(MemoryPoolCapacity, ChunkSize)
  private val masterPartitionChunks = new util.HashMap[String, DoubleChunk]()
  private val slavePartitionChunks = new util.HashMap[String, DoubleChunk]()

  // Threads
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")
  private var registerMasterFuture: JFuture[_] = null
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None

  // Configs
  private val HEARTBEAT_MILLIS = conf.getLong("jindo.worker.timeout", 60) * 1000 / 4
  private val REGISTRATION_RETRY_INTERVAL_SECONDS = 5
  private val TOTAL_REGISTRATION_RETRIES = 10

  // Structs
  var memoryUsed = 0
  def memoryFree: Int = memory - memoryUsed

  private val workerId = "worker-%s-%s-%d".format(Utils.createDateFormat.format(new Date), host, port)
  private var master: Option[RpcEndpointRef] = None

  override def onStart(): Unit = {
    assert(!registered)
    logInfo("Starting Jindo worker %s:%d with %s RAM".format(
      host, port, Utils.megabytesToString(memory)))
    registerWithMaster()
  }

  override def onStop(): Unit = {
    cancelLastRegistrationRetry()
    forwordMessageScheduler.shutdownNow()
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (masterRpcAddress == remoteAddress)  {
      logInfo(s"Master $remoteAddress Disassociated !")
      connected = false
      registerWithMaster()
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case msg: RegisterWorkerResponse =>
      handleRegisterResponse(msg)

    case ReregisterWithMaster =>
      reregisterWithMaster()

    case SendHeartbeat =>
      if (connected) { sendToMaster(Heartbeat(workerId, self)) }
  }

  var flag = false

  // TODO
  override def receiveAndReply(context: _root_.com.aliyun.emr.jss.common.rpc.RpcCallContext): _root_.scala.PartialFunction[Any, Unit] = {
    case RegisterPartition(applicationId, shuffleId, reduceId,
      partitionMemoryBytes, mode, chunkIndex) =>
      context.reply(RegisterPartitionResponse(true))
    case ReserveBuffers(masterLocations, slaveLocations) =>
      val masterDoubleChunks = new util.HashMap[String, DoubleChunk]()
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
                new DoubleChunk(ch1, ch2, memoryPool, masterLocations.get(ind).getUUID))
            }
          }
        }
      })
      if (masterDoubleChunks.size() < masterLocations.size()) {
        logInfo("not all master partition satisfied, return chunks")
        masterDoubleChunks.foreach(entry => entry._2.returnChunks())
        context.reply(ReserveBuffersResponse(false))
      } else {
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
          masterDoubleChunks.foreach(entry => entry._2.returnChunks())
          slaveDoubleChunks.foreach(entry => entry._2.returnChunks())
          context.reply(ReserveBuffersResponse(false))
        } else {
          masterPartitionChunks.synchronized {
            masterPartitionChunks.putAll(masterDoubleChunks)
          }
          slavePartitionChunks.synchronized {
            slavePartitionChunks.putAll(slaveDoubleChunks)
          }
          context.reply(ReserveBuffersResponse(true))
        }
      }
    case ClearBuffers(masterLocations, slaveLocations) =>
      context.reply(ClearBuffersResponse(true))
  }

  private def tryRegisterMaster(): JFuture[_] = {
    forwordMessageScheduler.submit(new Runnable {
      override def run(): Unit = {
        try {
          logInfo("Connecting to master " + masterRpcAddress + "...")
          val masterEndpoint = rpcEnv.setupEndpointRef(masterRpcAddress, RpcNameConstants.MASTER_EP)
          sendRegisterMessageToMaster(masterEndpoint)
        } catch {
          case ie: InterruptedException => // Cancelled
          case NonFatal(e) => logWarning(s"Failed to connect to master $masterRpcAddress", e)
        }
      }
    })
  }

  /**
    * Re-register with the master because a network failure or a master failure has occurred.
    * If the re-registration attempt threshold is exceeded, the worker exits with error.
    * Note that for thread-safety this should only be called from the rpcEndpoint.
    */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registered) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        /**
          * Re-register with the active master this worker has been communicating with. If there
          * is none, then it means this worker is still bootstrapping and hasn't established a
          * connection with a master yet, in which case we should re-register with all masters.
          *
          * It is important to re-register only with the active master during failures. Otherwise,
          * if the worker unconditionally attempts to re-register with all masters, the following
          * race condition may arise and cause a "duplicate worker" error detailed in SPARK-4592:
          *
          *   (1) Master A fails and Worker attempts to reconnect to all masters
          *   (2) Master B takes over and notifies Worker
          *   (3) Worker responds by registering with Master B
          *   (4) Meanwhile, Worker's previous reconnection attempt reaches Master B,
          *       causing the same Worker to register with Master B twice
          *
          * Instead, if we only register with the known active master, we can assume that the
          * old master must have died because another master has taken over. Note that this is
          * still not safe if the old master recovers within this interval, but this is a much
          * less likely scenario.
          */
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            if (registerMasterFuture != null) {
              registerMasterFuture.cancel(true)
            }
            registerMasterFuture = forwordMessageScheduler.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Connecting to master " + masterRpcAddress + "...")
                  val masterEndpoint = rpcEnv.setupEndpointRef(masterRpcAddress, RpcNameConstants.MASTER_EP)
                  sendRegisterMessageToMaster(masterEndpoint)
                } catch {
                  case ie: InterruptedException => // Cancelled
                  case NonFatal(e) => logWarning(s"Failed to connect to master $masterRpcAddress", e)
                }
              }
            })
          case None =>
            if (registerMasterFuture != null) {
              registerMasterFuture.cancel(true)
            }
            // We are retrying the initial registration
            registerMasterFuture = tryRegisterMaster()
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  /**
    * Cancel last registeration retry, or do nothing if no retry
    */
  private def cancelLastRegistrationRetry(): Unit = {
    if (registerMasterFuture != null) {
      registerMasterFuture.cancel(true)
      registerMasterFuture = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  private def registerWithMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registered = false
        registerMasterFuture = tryRegisterMaster()
        connectionAttemptCount = 0
        registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              Option(self).foreach(_.send(ReregisterWithMaster))
            }
          },
          REGISTRATION_RETRY_INTERVAL_SECONDS,
          REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  private def sendRegisterMessageToMaster(masterEndpoint: RpcEndpointRef): Unit = {
    masterEndpoint.send(RegisterWorker(
      workerId,
      host,
      port,
      memory,
      self
    ))
  }

  private def handleRegisterResponse(msg: RegisterWorkerResponse): Unit = {
    msg match {
      case RegisteredWorker =>
        logInfo("Successfully registered with master " + masterRpcAddress.toEssURL)
        registered = true
        master = Some(rpcEnv.setupEndpointRef(masterRpcAddress, RpcNameConstants.MASTER_EP))
        connected = true
        forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(SendHeartbeat)
          }
        }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)

      case RegisterWorkerFailed(message) =>
        if (!registered) {
          logError("Worker registration failed: " + message)
          System.exit(1)
        }
    }
  }

  private def sendToMaster(message: Any): Unit = {
    master match {
      case Some(masterRef) => masterRef.send(message)
      case None =>
        logWarning(
          s"Dropping $message because the connection to master has not yet been established")
    }
  }
}

private[deploy] object Worker extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new JindoConf
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
