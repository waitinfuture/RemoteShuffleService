package com.aliyun.emr.jss.service.deploy

import java.io.File
import java.util

import scala.collection.JavaConversions._

import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEnv}
import com.aliyun.emr.jss.common.util.Utils
import com.aliyun.emr.jss.common.EssConf
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ControlMessages._
import com.aliyun.emr.jss.protocol.message.DataMessages.{SendData, SendDataResponse}
import com.aliyun.emr.jss.protocol.message.ReturnCode
import com.aliyun.emr.jss.service.deploy.master.{Master, MasterArguments}
import com.aliyun.emr.jss.service.deploy.worker.{DoubleChunk, PartitionLocationWithDoubleChunks, Worker, WorkerArguments, WorkerInfo}
import org.scalatest.FunSuite

class MessageHandlerSuite extends FunSuite {
  /**
   * ===============================
   *         start master
   * ===============================
   */
  val conf = new EssConf()
  conf.set("ess.partition.memory", "128")
  val masterArgs = new MasterArguments(Array.empty[String], conf)
  val rpcEnvMaster: RpcEnv = RpcEnv.create(
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
   *         start worker1
   * ===============================
   */
  val workerArgs = new WorkerArguments(Array.empty[String], conf)
  val rpcEnvWorker1: RpcEnv = RpcEnv.create(RpcNameConstants.WORKER_SYS,
    workerArgs.host,
    9097,
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
   *         start worker2
   * ===============================
   */
  val workerArgs2 = new WorkerArguments(Array.empty[String], conf)
  val rpcEnvWorker2: RpcEnv = RpcEnv.create(RpcNameConstants.WORKER_SYS,
    workerArgs2.host,
    9098,
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

  val localhost = Utils.localHostName()
  val env = RpcEnv.create(
    "MessageHandlerSuite",
    localhost,
    0,
    conf
  )
  val master = env.setupEndpointRef(new RpcAddress(localhost, 9099), RpcNameConstants.MASTER_EP)
  val worker1 = env.setupEndpointRef(new RpcAddress(localhost, 9097), RpcNameConstants.WORKER_EP)
  val worker2 = env.setupEndpointRef(new RpcAddress(localhost, 9098), RpcNameConstants.WORKER_EP)

  /**
   * ===============================
   *         start testing
   * ===============================
   */
  var partitionLocations: util.List[PartitionLocation] = _

  // TODO each test has full pepeline
  test("RegisterShuffle") {
    val res = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(
        "appId",
        1,
        10,
        10
      )
    )
    assert(res.success)
    partitionLocations = res.partitionLocations
    assert(partitionLocations.size() == 10)
    partitionLocations.foreach(p => {
      assert(p.getMode == PartitionLocation.Mode.Master)
      assert(p.getPeer != null)
      assert(p.getPeer.getMode == PartitionLocation.Mode.Slave)
      println(p)
    })
  }

  test("GetWorkerInfos") {
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
  }

  test("SendData") {
    val bytes = new Array[Byte](64)
    0 until bytes.length foreach (ind => bytes(ind) = 'a')
    val worker1Location = partitionLocations.filter(p => p.getPort == 9097).toList(0)
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

    val worker2Location = partitionLocations.filter(p => p.getPort == 9098).toList(0)
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

    val worker2Location2 = partitionLocations.filter(p => p.getPort == 9098).toList(1)
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
  }

  test("MapperEnd") {
    val partitionIds = new util.ArrayList[String]()
    partitionIds.add("p1")
    partitionIds.add("p2")
    val res = master.askSync[MapperEndResponse](
      MapperEnd(
        "appId",
        1,
        0,
        1,
        partitionIds
      )
    )
    assert(res.success)
    println(res)
  }

  // TODO redesign
  test("SlaveLost") {
    val partitionId = "p1"
    val masterLocation = new PartitionLocation(
      partitionId,
      localhost,
      11,
      PartitionLocation.Mode.Master
    )
    val slaveLocation = new PartitionLocation(
      partitionId,
      localhost,
      10,
      PartitionLocation.Mode.Slave,
      masterLocation
    )
    masterLocation.setPeer(slaveLocation)
    val res = master.askSync[SlaveLostResponse](
      SlaveLost("appId-1", masterLocation, slaveLocation)
    )
    println(res)
  }

  test("Destroy-All") {
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
    partitionLocations = res.partitionLocations
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
  }

  test("Destroy-Some") {
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
    partitionLocations = res.partitionLocations
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
  }

  // TODO test SlaveLost
}
