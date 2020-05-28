package com.aliyun.emr.jss.service.deploy

import java.io.File
import java.util

import scala.collection.JavaConversions._
import scala.util.Random

import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import com.aliyun.emr.jss.common.util.Utils
import com.aliyun.emr.jss.common.EssConf
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ControlMessages._
import com.aliyun.emr.jss.protocol.message.DataMessages.{GetDoubleChunkInfo, GetDoubleChunkInfoResponse, SendData, SendDataResponse}
import com.aliyun.emr.jss.protocol.message.ReturnCode
import com.aliyun.emr.jss.service.deploy.master.{Master, MasterArguments}
import com.aliyun.emr.jss.service.deploy.worker.{DoubleChunk, PartitionLocationWithDoubleChunks, Worker, WorkerArguments, WorkerInfo}
import org.scalatest.FunSuite

class MessageHandlerSuite
  extends FunSuite {
  def init(numWorkers: Int = 2) {
    /**
     * ===============================
     * start master
     * ===============================
     */
    val conf = new EssConf()
    conf.set("ess.partition.memory", "128")
    val masterArgs = new MasterArguments(Array.empty[String], conf)
    rpcEnvMaster = RpcEnv.create(
      RpcNameConstants.MASTER_SYS,
      masterArgs.host,
      masterArgs.port,
      conf)
    rpcEnvMaster.setupEndpoint(RpcNameConstants.MASTER_EP,
      new Master(rpcEnvMaster, rpcEnvMaster.address, conf))

    new Thread() {
      override def run(): Unit = {
        rpcEnvMaster.awaitTermination()
      }
    }.start()
    Thread.sleep(1000)
    println("started master")

    /**
     * ===============================
     * start worker1
     * ===============================
     */
    val workerArgs = new WorkerArguments(Array.empty[String], conf)
    rpcEnvWorker1 = RpcEnv.create(RpcNameConstants.WORKER_SYS,
      workerArgs.host,
      port1,
      conf)

    val masterAddresses: RpcAddress = RpcAddress.fromJindoURL(workerArgs.master)
    rpcEnvWorker1.setupEndpoint(RpcNameConstants.WORKER_EP,
      new Worker(rpcEnvWorker1, workerArgs.memory,
        masterAddresses, RpcNameConstants.WORKER_EP, conf))

    new Thread() {
      override def run(): Unit = {
        rpcEnvWorker1.awaitTermination()
      }
    }.start()
    Thread.sleep(1000)
    println("started worker1")

    /**
     * ===============================
     * start worker2
     * ===============================
     */
    val workerArgs2 = new WorkerArguments(Array.empty[String], conf)
    rpcEnvWorker2 = RpcEnv.create(RpcNameConstants.WORKER_SYS,
      workerArgs2.host,
      port2,
      conf)

    rpcEnvWorker2.setupEndpoint(RpcNameConstants.WORKER_EP,
      new Worker(rpcEnvWorker2, workerArgs.memory,
        masterAddresses, RpcNameConstants.WORKER_EP, conf))

    new Thread() {
      override def run(): Unit = {
        rpcEnvWorker2.awaitTermination()
      }
    }.start()
    Thread.sleep(1000)
    println("started worker2")

    /**
     * ===============================
     * start worker3 if needed
     * ===============================
     */
    val workerArgs3 = new WorkerArguments(Array.empty[String], conf)
    rpcEnvWorker3 = RpcEnv.create(RpcNameConstants.WORKER_SYS,
      workerArgs3.host,
      port3,
      conf)

    rpcEnvWorker3.setupEndpoint(RpcNameConstants.WORKER_EP,
      new Worker(rpcEnvWorker3, workerArgs.memory,
        masterAddresses, RpcNameConstants.WORKER_EP, conf))

    new Thread() {
      override def run(): Unit = {
        rpcEnvWorker3.awaitTermination()
      }
    }.start()
    Thread.sleep(1000)
    println("started worker3")


    val localhost = Utils.localHostName()
    val env = RpcEnv.create(
      "MessageHandlerSuite",
      localhost,
      0,
      conf
    )
    master = env.setupEndpointRef(new RpcAddress(localhost, masterPort), RpcNameConstants.MASTER_EP)
    worker1 = env.setupEndpointRef(new RpcAddress(localhost, port1), RpcNameConstants.WORKER_EP)
    worker2 = env.setupEndpointRef(new RpcAddress(localhost, port2), RpcNameConstants.WORKER_EP)
    if (numWorkers == 3) {
      worker3 = env.setupEndpointRef(new RpcAddress(localhost, port3), RpcNameConstants.WORKER_EP)
    }
  }

  def stop(numWorkers: Int = 2): Unit = {
    rpcEnvWorker1.shutdown()
    rpcEnvWorker2.shutdown()
    if (numWorkers == 3) {
      rpcEnvWorker3.shutdown()
    }
    rpcEnvMaster.shutdown()
  }

  def getWorker(loc: PartitionLocation): RpcEndpointRef = {
    if (loc.getPort == port1) {
      worker1
    } else if (loc.getPort == port2) {
      worker2
    } else if (loc.getPort == port3) {
      worker3
    } else {
      null
    }
  }

  val masterPort = 9099
  val port1 = 9090
  val port2 = 9091
  val port3 = 9092

  var rpcEnvMaster: RpcEnv = _
  var rpcEnvWorker1: RpcEnv = _
  var rpcEnvWorker2: RpcEnv = _
  var rpcEnvWorker3: RpcEnv = _
  var master: RpcEndpointRef = _
  var worker1: RpcEndpointRef = _
  var worker2: RpcEndpointRef = _
  var worker3: RpcEndpointRef = _

  /**
   * ===============================
   * start testing
   * ===============================
   */

  // TODO each test has full pepeline
  test("RegisterShuffle") {
    init()

    val res = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(
        "appId",
        1,
        10,
        10
      )
    )
    assert(res.success)
    val partitionLocations = res.partitionLocations
    assert(partitionLocations.size() == 10)
    partitionLocations.foreach(p => {
      assert(p.getMode == PartitionLocation.Mode.Master)
      assert(p.getPeer != null)
      assert(p.getPeer.getMode == PartitionLocation.Mode.Slave)
      println(p)
    })

    stop()
  }

  test("GetWorkerInfos") {
    init()

    master.askSync[RegisterShuffleResponse](
      RegisterShuffle(
        "appId",
        1,
        10,
        10
      )
    )
    // Master
    var res = master.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res.success)
    val workerInfos = res.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.length == 2)
    assert(workerInfos(0).memoryUsed == 128 * 10)
    assert(workerInfos.get(0).masterPartitionLocations.size() == 1)
    assert(workerInfos.get(0).masterPartitionLocations.contains("appId-1"))
    assert(workerInfos.get(0).masterPartitionLocations.get("appId-1").size() == 5)
    assert(workerInfos.get(0).slavePartitionLocations.size() == 1)
    assert(workerInfos.get(0).slavePartitionLocations.contains("appId-1"))
    assert(workerInfos.get(0).slavePartitionLocations.get("appId-1").size() == 5)

    assert(workerInfos(1).memoryUsed == 128 * 10)
    assert(workerInfos.get(1).masterPartitionLocations.size() == 1)
    assert(workerInfos.get(1).masterPartitionLocations.contains("appId-1"))
    assert(workerInfos.get(1).masterPartitionLocations.get("appId-1").size() == 5)
    assert(workerInfos.get(1).slavePartitionLocations.size() == 1)
    assert(workerInfos.get(1).slavePartitionLocations.contains("appId-1"))
    assert(workerInfos.get(1).slavePartitionLocations.get("appId-1").size() == 5)

    // Worker
    res = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res.success)
    assert(res.workerInfos.asInstanceOf[util.List[WorkerInfo]].size() == 1)
    val worker = res.workerInfos.asInstanceOf[util.List[WorkerInfo]].get(0)
    assert(worker.memoryUsed == 1280)
    assert(worker.masterPartitionLocations.size() == 1)
    assert(worker.slavePartitionLocations.size() == 1)
    assert(worker.masterPartitionLocations.contains("appId-1"))
    assert(worker.slavePartitionLocations.contains("appId-1"))
    assert(worker.masterPartitionLocations.get("appId-1").size() == 5)
    assert(worker.slavePartitionLocations.get("appId-1").size() == 5)
    val partitionLocationWithDoubleChunks =
      worker.masterPartitionLocations.get("appId-1").valuesIterator.next()
        .asInstanceOf[PartitionLocationWithDoubleChunks]
    assert(partitionLocationWithDoubleChunks.getDoubleChunk.slaveState ==
      DoubleChunk.ChunkState.Ready)

    stop()
  }

  test("SendData") {
    init()

    val resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(
        "appId",
        1,
        10,
        10
      )
    )
    val partitionLocations = resReg.partitionLocations

    val bytes = new Array[Byte](64)
    0 until bytes.length foreach (ind => bytes(ind) = 'a')
    val worker1Location = partitionLocations.filter(p => p.getPort == port1).toList(0)
    val file1 = new File(worker1Location.getUUID)
    assert(file1.length() == 0)
    val sendDataMsg1 = SendData(
      "appId-1",
      worker1Location,
      PartitionLocation.Mode.Master,
      true,
      bytes
    )
    var res = worker1.askSync[SendDataResponse](sendDataMsg1)
    assert(res.success)
    res = worker1.askSync[SendDataResponse](sendDataMsg1)
    assert(res.success)
    Thread.sleep(100)
    assert(file1.length() == 0)
    res = worker1.askSync[SendDataResponse](sendDataMsg1)
    assert(res.success)
    Thread.sleep(100)
    assert(file1.length() == 128)
    res = worker1.askSync[SendDataResponse](sendDataMsg1)
    assert(res.success)
    Thread.sleep(100)
    assert(file1.length() == 128)
    res = worker1.askSync[SendDataResponse](sendDataMsg1)
    assert(res.success)
    Thread.sleep(100)
    assert(file1.length() == 256)

    val worker2Location = partitionLocations.filter(p => p.getPort == port2).toList(0)
    val file2 = new File(worker2Location.getUUID)
    assert(file2.length() == 0)
    val sendDataMsg2 = SendData(
      "appId-1",
      worker2Location,
      PartitionLocation.Mode.Master,
      true,
      bytes
    )
    res = worker2.askSync[SendDataResponse](sendDataMsg2)
    assert(res.success)
    Thread.sleep(100)
    assert(file2.length() == 0)
    res = worker2.askSync[SendDataResponse](sendDataMsg2)
    assert(res.success)
    Thread.sleep(100)
    assert(file2.length() == 0)
    res = worker2.askSync[SendDataResponse](sendDataMsg2)
    assert(res.success)
    Thread.sleep(100)
    assert(file2.length() == 128)
    res = worker2.askSync[SendDataResponse](sendDataMsg2)
    assert(res.success)
    Thread.sleep(100)
    assert(file2.length() == 128)
    res = worker2.askSync[SendDataResponse](sendDataMsg2)
    assert(res.success)
    Thread.sleep(100)
    assert(file2.length() == 256)

    val worker2Location2 = partitionLocations.filter(p => p.getPort == port2).toList(1)
    val file3 = new File(worker2Location2.getUUID)
    assert(file3.length() == 0)
    val sendDataMsg3 = SendData(
      "appId-1",
      worker2Location2,
      PartitionLocation.Mode.Master,
      true,
      bytes
    )
    res = worker2.askSync[SendDataResponse](sendDataMsg3)
    assert(res.success)
    Thread.sleep(100)
    assert(file3.length() == 0)
    res = worker2.askSync[SendDataResponse](sendDataMsg3)
    assert(res.success)
    Thread.sleep(100)
    assert(file3.length() == 0)
    res = worker2.askSync[SendDataResponse](sendDataMsg3)
    assert(res.success)
    Thread.sleep(100)
    assert(file3.length() == 128)
    res = worker2.askSync[SendDataResponse](sendDataMsg3)
    assert(res.success)
    Thread.sleep(100)
    assert(file3.length() == 128)
    res = worker2.askSync[SendDataResponse](sendDataMsg3)
    assert(res.success)
    Thread.sleep(100)
    assert(file3.length() == 256)

    val res2 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res2.success)
    val workerInfos = res2.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.length == 1)
    val worker = workerInfos.get(0)
    assert(worker.masterPartitionLocations.size() == 1)
    assert(worker.masterPartitionLocations.contains("appId-1"))
    assert(worker.masterPartitionLocations.get("appId-1").contains(worker2Location2))
    val partitionLocationWithDoubleChunks = worker.masterPartitionLocations.get("appId-1")
      .get(worker2Location2)
    val doubleChunk = partitionLocationWithDoubleChunks
      .asInstanceOf[PartitionLocationWithDoubleChunks].getDoubleChunk
    assert(doubleChunk.working == 0)
    assert(doubleChunk.slaveState == DoubleChunk.ChunkState.Ready)
    assert(doubleChunk.fileName.equals(worker2Location2.getUUID))

    stop()
  }

  test("MapperEnd") {
    init()

    val resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(
        "appId",
        1,
        10,
        10
      )
    )
    val partitionLocations = resReg.partitionLocations

    val res = master.askSync[MapperEndResponse](
      MapperEnd(
        "appId",
        1,
        0,
        1,
        partitionLocations.map(_.getUUID)
      )
    )
    assert(res.success)

    stop()
  }

  test("Destroy-All") {
    init()

    val applicationId = "Destroy-Test"
    val shuffleId = 1
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    val res = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(
        applicationId,
        shuffleId,
        3,
        6
      )
    )

    assert(res.success)
    val partitionLocations = res.partitionLocations
    assert(partitionLocations.size() == 6)
    partitionLocations.foreach(p => {
      assert(p.getMode == PartitionLocation.Mode.Master)
      assert(p.getPeer != null)
      assert(p.getPeer.getMode == PartitionLocation.Mode.Slave)
      println(p)
    })

    var res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.success)
    var workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.size() == 1)
    var workerInfo = workerInfos.get(0)
    assert(workerInfo.masterPartitionLocations.contains(shuffleKey))
    assert(workerInfo.masterPartitionLocations.get(shuffleKey).size() == 3)
    var res2 = worker1.askSync[DestroyResponse](
      Destroy(shuffleKey,
        workerInfo.masterPartitionLocations.get(shuffleKey).keySet().toList,
        workerInfo.slavePartitionLocations.get(shuffleKey).keySet().toList)
    )
    assert(res2.returnCode == ReturnCode.Success)

    res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.success)
    workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.size() == 1)
    workerInfo = workerInfos.get(0)
    assert(workerInfo.masterPartitionLocations.get(shuffleKey) == null)
    assert(workerInfo.slavePartitionLocations.get(shuffleKey) == null)
    assert(workerInfo.memoryUsed == 0)

    res1 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.success)
    workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    workerInfo = workerInfos.get(0)
    assert(workerInfo.masterPartitionLocations.get(shuffleKey).size() == 3)
    assert(workerInfo.slavePartitionLocations.get(shuffleKey).size() == 3)
    res2 = worker2.askSync[DestroyResponse](
      Destroy(shuffleKey,
        workerInfo.masterPartitionLocations.get(shuffleKey).keySet().toList,
        workerInfo.slavePartitionLocations.get(shuffleKey).keySet().toList)
    )
    assert(res2.returnCode == ReturnCode.Success)

    res1 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.success)
    workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    workerInfo = workerInfos.get(0)
    assert(workerInfo.masterPartitionLocations.get(shuffleKey) == null)
    assert(workerInfo.slavePartitionLocations.get(shuffleKey) == null)

    stop()
  }

  test("Destroy-Some") {
    init()

    val applicationId = "Destroy-Test"
    val shuffleId = 1
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    val res = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(
        applicationId,
        shuffleId,
        3,
        6
      )
    )

    assert(res.success)
    val partitionLocations = res.partitionLocations
    assert(partitionLocations.size() == 6)
    partitionLocations.foreach(p => {
      assert(p.getMode == PartitionLocation.Mode.Master)
      assert(p.getPeer != null)
      assert(p.getPeer.getMode == PartitionLocation.Mode.Slave)
      println(p)
    })

    var res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.success)
    var workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.size() == 1)
    var workerInfo = workerInfos.get(0)
    assert(workerInfo.masterPartitionLocations.contains(shuffleKey))
    assert(workerInfo.masterPartitionLocations.get(shuffleKey).size() == 3)
    val res2 = worker1.askSync[DestroyResponse](
      Destroy(shuffleKey,
        List(workerInfo.masterPartitionLocations.get(shuffleKey).keySet().head),
        List(workerInfo.slavePartitionLocations.get(shuffleKey).keySet().head))
    )
    assert(res2.returnCode == ReturnCode.Success)

    res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.success)
    workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.size() == 1)
    workerInfo = workerInfos.get(0)
    assert(workerInfo.masterPartitionLocations.get(shuffleKey).size() == 2)
    assert(workerInfo.slavePartitionLocations.get(shuffleKey).size() == 2)
    assert(workerInfo.memoryUsed == 128 * 4)

    stop()
  }

  test("SlaveLost") {
    init()

    // register shuffle
    val applicationId = "slavelost"
    val shuffleId = 2
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    val res = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(applicationId, shuffleId, 10, 10)
    )
    assert(res.success)

    // get partition info
    var res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.success)
    assert(res1.workerInfos.asInstanceOf[util.List[WorkerInfo]].size() == 1)
    var workerInfo = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]].get(0)
    assert(workerInfo.memoryUsed == 128 * 10)
    assert(workerInfo.masterPartitionLocations.size() == 1)
    assert(workerInfo.slavePartitionLocations.size() == 1)
    assert(workerInfo.masterPartitionLocations.contains(shuffleKey))
    var masterLocations = workerInfo.masterPartitionLocations.get(shuffleKey)

    res1 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.success)
    workerInfo = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]].get(0)
    assert(workerInfo.memoryUsed == 128 * 10)
    var slaveLocations = workerInfo.slavePartitionLocations.get(shuffleKey)

    masterLocations.keySet().foreach(loc => assert(slaveLocations.contains(loc.getPeer)))

    // send data before trigger SlaveLost
    val lostSlave = masterLocations.head._1.getPeer
    0 until 5 foreach (_ => {
      val data = new Array[Byte](63)
      Random.nextBytes(data)
      worker1.askSync[SendDataResponse](
        SendData(
          shuffleKey,
          lostSlave.getPeer,
          PartitionLocation.Mode.Master,
          true,
          data
        )
      )
    })

    // trigger slave lost
    var res2 = worker1.send(
      SlaveLost(shuffleKey, lostSlave.getPeer, lostSlave)
    )
    Thread.sleep(500)
    res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    workerInfo = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]].get(0)
    assert(workerInfo.memoryUsed == 128 * 10)
    masterLocations = workerInfo.masterPartitionLocations.get(shuffleKey)

    res1 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    workerInfo = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]].get(0)
    assert(workerInfo.memoryUsed == 128 * 10)
    slaveLocations = workerInfo.slavePartitionLocations.get(shuffleKey)

    assert(masterLocations.size() == 5)
    assert(slaveLocations.size() == 5)

    val masterLocation = masterLocations.get(lostSlave.getPeer)
    val newPeer = masterLocation.getPeer
    assert(slaveLocations.contains(newPeer))
    // since we have only 2 workers, newPeer should be equal to lostSlave
    assert(lostSlave.equals(newPeer))
    assert(slaveLocations.contains(newPeer))
    val masterDoubleChunkInfo = worker1.askSync[GetDoubleChunkInfoResponse](
      GetDoubleChunkInfo(shuffleKey, PartitionLocation.Mode.Master, masterLocation)
    )
    val slaveDoubleChunkInfo = worker2.askSync[GetDoubleChunkInfoResponse](
      GetDoubleChunkInfo(shuffleKey, PartitionLocation.Mode.Slave, newPeer)
    )
    assert(masterDoubleChunkInfo.success)
    assert(slaveDoubleChunkInfo.success)
    assert(masterDoubleChunkInfo.working == slaveDoubleChunkInfo.working)
    assert(masterDoubleChunkInfo.masterRemaining == slaveDoubleChunkInfo.masterRemaining)
    assert(masterDoubleChunkInfo.slaveRemaining == slaveDoubleChunkInfo.slaveRemaining)
    assert(masterDoubleChunkInfo.masterData.size == slaveDoubleChunkInfo.masterData.size)
    assert(masterDoubleChunkInfo.slaveData.size == slaveDoubleChunkInfo.slaveData.size)
    masterDoubleChunkInfo.masterData.zipWithIndex.foreach(entry => {
      assert(entry._1 == slaveDoubleChunkInfo.masterData(entry._2))
    })
    masterDoubleChunkInfo.slaveData.zipWithIndex.foreach(entry => {
      assert(entry._1 == slaveDoubleChunkInfo.slaveData(entry._2))
    })
    assert(masterDoubleChunkInfo.working == 0)
    assert(masterDoubleChunkInfo.masterRemaining == 65)
    assert(masterDoubleChunkInfo.slaveRemaining == 128)

    stop()
  }

  test("WorkerLost-2Workers") {
    init()

    val shuffleKey = Utils.makeShuffleKey("appId", 0)
    val resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(
        "appId",
        0,
        10,
        10
      )
    )

    val locations = resReg.partitionLocations

    0 until 5 foreach (ind => {
      val data = new Array[Byte](63)
      Random.nextBytes(data)
      val res = worker1.askSync[SendDataResponse](
        SendData(
          shuffleKey,
          locations.get(ind),
          PartitionLocation.Mode.Master,
          true,
          data
        )
      )
      assert(res.success)
    })

    5 until 10 foreach (ind => {
      val data = new Array[Byte](63)
      Random.nextBytes(data)
      val res = worker2.askSync[SendDataResponse](
        SendData(
          shuffleKey,
          locations.get(ind),
          PartitionLocation.Mode.Master,
          true,
          data
        )
      )
      assert(res.success)
    })

    val res = master.askSync[WorkerLostResponse](
      WorkerLost(locations.head.getHost, port1)
    )
    assert(res.success)
    Thread.sleep(1000)

    var res1 = master.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    var workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.size() == 1)
    var workerInfo = workerInfos.get(0)
    assert(workerInfo.masterPartitionLocations.get(shuffleKey).size() == 0)
    assert(workerInfo.slavePartitionLocations.get(shuffleKey).size() == 0)
    workerInfo.masterPartitionLocations.get(shuffleKey).keySet().foreach(loc => {
      assert(loc.getPeer == null)
    })
    assert(workerInfo.memoryUsed == 0)

    res1 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.size() == 1)
    workerInfo = workerInfos.get(0)
    assert(workerInfo.masterPartitionLocations.get(shuffleKey).size() == 0)
    assert(!workerInfo.slavePartitionLocations.contains(shuffleKey))
    workerInfo.masterPartitionLocations.get(shuffleKey).keySet().foreach(loc => {
      assert(loc.getPeer == null)
    })
    assert(workerInfo.memoryUsed == 0)

    stop()
  }

  test("WorkerLost-3Workers") {
    init(3)

    // register shuffle
    val appId = "appId"
    val shuffleId = 1
    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
    val resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId, 10, 12)
    )
    assert(resReg.success)

    /**
     * check worker info
     */
    var worker1InfoMaster: WorkerInfo = null
    var worker2InfoMaster: WorkerInfo = null
    var worker3InfoMaster: WorkerInfo = null

    var worker1InfoWorker: WorkerInfo = null
    var worker2InfoWorker: WorkerInfo = null
    var worker3InfoWorker: WorkerInfo = null

    def getInfosInMaster(): Unit = {
      val workerInfosMaster = master.askSync[GetWorkerInfosResponse](GetWorkerInfos).workerInfos
        .asInstanceOf[util.List[WorkerInfo]]
      worker1InfoMaster = null
      worker2InfoMaster = null
      worker3InfoMaster = null
      if (workerInfosMaster != null) {
        workerInfosMaster.foreach(worker => {
          if (worker.port == port1) {
            worker1InfoMaster = worker
          } else if (worker.port == port2) {
            worker2InfoMaster = worker
          } else if (worker.port == port3) {
            worker3InfoMaster = worker
          }
        })
      }
    }

    def getInfosInWorker(): Unit = {
      worker1InfoWorker = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos).workerInfos
        .asInstanceOf[util.List[WorkerInfo]].get(0)
      worker2InfoWorker = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos).workerInfos
        .asInstanceOf[util.List[WorkerInfo]].get(0)
      worker3InfoWorker = worker3.askSync[GetWorkerInfosResponse](GetWorkerInfos).workerInfos
        .asInstanceOf[util.List[WorkerInfo]].get(0)
    }

    def assertInfos(): Unit = {
      getInfosInMaster()
      getInfosInWorker()

      // assert worker1 info same on both worker and master
      assert(worker1InfoMaster.hasSameInfoWith(worker1InfoWorker))
      assert(worker2InfoMaster.hasSameInfoWith(worker2InfoWorker))
      assert(worker3InfoMaster.hasSameInfoWith(worker3InfoWorker))
    }

    assertInfos()

    assert(worker1InfoMaster.masterPartitionLocations.get(shuffleKey).size() == 4)
    assert(worker2InfoMaster.masterPartitionLocations.get(shuffleKey).size() == 4)
    assert(worker3InfoMaster.masterPartitionLocations.get(shuffleKey).size() == 4)
    assert(worker1InfoMaster.slavePartitionLocations.get(shuffleKey).size() == 4)
    assert(worker2InfoMaster.slavePartitionLocations.get(shuffleKey).size() == 4)
    assert(worker3InfoMaster.slavePartitionLocations.get(shuffleKey).size() == 4)

    // send data
    0 until 5 foreach (_ => {
      resReg.partitionLocations.foreach(loc => {
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val res = getWorker(loc).askSync[SendDataResponse](
          SendData(shuffleKey, loc, PartitionLocation.Mode.Master, true, data)
        )
        assert(res.success)
      })
    })

    assertInfos()

    // trigger worker lost
    master.askSync[WorkerLostResponse](
      WorkerLost(worker1.address.host, worker1.address.port)
    )
    Thread.sleep(500)
    getInfosInMaster()
    getInfosInWorker()
    if (worker1InfoMaster != null) {
      assert(worker1InfoMaster.hasSameInfoWith(worker1InfoWorker))
    }
    if (worker2InfoMaster != null) {
      assert(worker2InfoMaster.hasSameInfoWith(worker2InfoWorker))
    }
    if (worker3InfoMaster != null) {
      assert(worker3InfoMaster.hasSameInfoWith(worker3InfoWorker))
    }

    // trigger another worker lost
    master.askSync[WorkerLostResponse](
      WorkerLost(worker2.address.host, worker2.address.port)
    )
    Thread.sleep(1000)
    getInfosInMaster()
    getInfosInWorker()
    if (worker1InfoMaster != null) {
      assert(worker1InfoMaster.hasSameInfoWith(worker1InfoWorker))
    }
    if (worker2InfoMaster != null) {
      assert(worker2InfoMaster.hasSameInfoWith(worker2InfoWorker))
    }
    if (worker3InfoMaster != null) {
      assert(worker3InfoMaster.hasSameInfoWith(worker3InfoWorker))
    }

    stop(3)
  }
}
