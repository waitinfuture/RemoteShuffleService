package com.aliyun.emr.jss.service.deploy

import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.Random
import com.aliyun.emr.jss.client.impl.ShuffleClientImpl
import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import com.aliyun.emr.jss.common.util.Utils
import com.aliyun.emr.jss.common.EssConf
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ControlMessages._
import com.aliyun.emr.jss.protocol.message.DataMessages.{GetDoubleChunkInfo, GetDoubleChunkInfoResponse, PushDataResponse}
import com.aliyun.emr.jss.protocol.message.StatusCode
import com.aliyun.emr.jss.service.deploy.common.EssPathUtil
import com.aliyun.emr.jss.service.deploy.master.{Master, MasterArguments}
import com.aliyun.emr.jss.service.deploy.worker._
import com.aliyun.emr.network.buffer.NettyManagedBuffer
import com.aliyun.emr.network.client.TransportClient
import com.aliyun.emr.network.protocol.ess.PushData
import io.netty.buffer.Unpooled
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.FunSuite

class MessageHandlerSuite
  extends FunSuite {
  def init(numWorkers: Int = 2) {
    /**
     * ===============================
     * start master
     * ===============================
     */
    conf.set("ess.partition.memory", "128")
    conf.set("ess.worker.base.dir", "hdfs://11.158.199.162:9000/tmp/ess-test/")
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
    if (numWorkers >= 3) {
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

    }

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

    val file = new Path("hdfs://11.158.199.162:9000/tmp/ess-test/")
    val hadoopConf = new Configuration()
    fs = file.getFileSystem(hadoopConf)
  }

  def stop(numWorkers: Int = 2): Unit = {
    rpcEnvWorker1.shutdown()
    rpcEnvWorker2.shutdown()
    if (numWorkers == 3) {
      rpcEnvWorker3.shutdown()
    }
    rpcEnvMaster.shutdown()

    fs.close()
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

  val conf = new EssConf()

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

  var worker1InfoMaster: WorkerInfo = null
  var worker2InfoMaster: WorkerInfo = null
  var worker3InfoMaster: WorkerInfo = null

  var worker1InfoWorker: WorkerInfo = null
  var worker2InfoWorker: WorkerInfo = null
  var worker3InfoWorker: WorkerInfo = null

  var fs: FileSystem = null

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
    if (worker1InfoMaster != null) {
      assert(worker1InfoMaster.hasSameInfoWith(worker1InfoWorker))
    }
    if (worker2InfoMaster != null) {
      assert(worker2InfoMaster.hasSameInfoWith(worker2InfoWorker))
    }
    if (worker3InfoMaster != null) {
      assert(worker3InfoMaster.hasSameInfoWith(worker3InfoWorker))
    }
  }

  def assertChunkInfo(shuffleKey: String, masterLocations: util.List[PartitionLocation]): Unit = {
    masterLocations.foreach(loc => {
      val peer = loc.getPeer
      val worker1 = getWorker(loc)
      val worker2 = getWorker(peer)
      val info1 = worker1.askSync[GetDoubleChunkInfoResponse](
        GetDoubleChunkInfo(shuffleKey, PartitionLocation.Mode.Master, loc)
      )
      assert(info1.success)
      val info2 = worker2.askSync[GetDoubleChunkInfoResponse](
        GetDoubleChunkInfo(shuffleKey, PartitionLocation.Mode.Slave, peer)
      )
      assert(info2.success)
      assert(info1.working == info2.working)
      assert(info1.slaveRemaining == info2.slaveRemaining)
      assert(info1.masterRemaining == info2.masterRemaining)
      val masterData1 = info1.masterData
      val slaveData1 = info1.slaveData
      val masterData2 = info2.masterData
      val slaveData2 = info2.slaveData
      assert(masterData1.length == masterData2.length)
      masterData1.zipWithIndex.foreach(entry => {
        assert(entry._1 == masterData2(entry._2))
      })
      assert(slaveData1.length == slaveData2.length)
      slaveData1.zipWithIndex.foreach(entry => {
        assert(entry._1 == slaveData2(entry._2))
      })
    })
  }

  def getFileLengh(shuffleKey: String, reduceId: Int, name: String): Long = {
    val appId = shuffleKey.split("-").dropRight(1).mkString("-")
    val shuffleId = shuffleKey.split("-").last
    val file = new Path(String.format("%s/%s",
      s"hdfs://11.158.199.162:9000/tmp/ess-test/$appId/$shuffleId/$reduceId/",
      name))
    if (!fs.exists(file)) {
      println(s"file $file does not exists")
      0
    } else {
      val status = fs.getFileStatus(file)
      status.getLen()
    }
  }

  def assertFileLength(shuffleKey: String, locations: util.List[PartitionLocation], size: Int): Unit = {
    locations.foreach(loc => {
      assert(getFileLengh(shuffleKey, loc.getReduceId, loc.getUUID).toInt == size)
    })
  }

  def waitUntilDataFinishFlushing(worker: RpcEndpointRef, shuffleKey: String): Unit = {
    var res = worker.askSync[GetShuffleStatusResponse](
      GetShuffleStatus(shuffleKey)
    )
    while (res.dataWriting) {
      println("data is writing")
      Thread.sleep(500)
      res = worker.askSync[GetShuffleStatusResponse](
        GetShuffleStatus(shuffleKey)
      )
    }
  }

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
    assert(res.status.equals(StatusCode.Success))
    val partitionLocations = res.partitionLocations
    assert(partitionLocations.size() == 10)
    partitionLocations.foreach(p => {
      assert(p.getMode == PartitionLocation.Mode.Master)
      assert(p.getPeer != null)
      assert(p.getPeer.getMode == PartitionLocation.Mode.Slave)
    })

    assertResult((0 until 10).toList) {
      partitionLocations.map(_.getReduceId).sorted.toList
    }

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
    // Master WorkerInfos
    var res = master.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res.status.equals(StatusCode.Success))
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

    // assert master location reduceId
    assertResult((0 until 10).toList) {
      workerInfos.flatMap(w => {
        w.masterPartitionLocations.get("appId-1").keySet()
      }).map(_.getReduceId).sorted.toList
    }

    // assert slave location reduceId
    assertResult((0 until 10).toList) {
      workerInfos.flatMap(w => {
        w.slavePartitionLocations.get("appId-1").keySet()
      }).map(_.getReduceId).sorted.toList
    }

    // Worker WorkerInfos
    res = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res.status.equals(StatusCode.Success))
    assert(res.workerInfos.asInstanceOf[util.List[WorkerInfo]].size() == 1)
    val worker = res.workerInfos.asInstanceOf[util.List[WorkerInfo]].get(0)
    assert(worker.memoryUsed == 1280)
    assert(worker.masterPartitionLocations.size() == 1)
    assert(worker.slavePartitionLocations.size() == 1)
    assert(worker.masterPartitionLocations.contains("appId-1"))
    assert(worker.slavePartitionLocations.contains("appId-1"))
    assert(worker.masterPartitionLocations.get("appId-1").size() == 5)
    assert(worker.slavePartitionLocations.get("appId-1").size() == 5)

    stop()
  }

  test("PushData") {
    init()

    val appId = "appId"
    val shuffleId = 1
    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
    val resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(
        appId,
        shuffleId,
        10,
        10
      )
    )
    val partitionLocations = resReg.partitionLocations

    val bytes = new Array[Byte](64)
    0 until bytes.length foreach (ind => bytes(ind) = 'a')
    val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(bytes))
    val worker1Location = partitionLocations.filter(p => p.getPort == port1).toList(0)
    val pushDataMsg1 = new PushData(
      shuffleKey,
      worker1Location.getUUID,
      PartitionLocation.Mode.Master.mode,
      buf,
      TransportClient.requestId()
    )
    buf.retain()
    var res1 = worker1.pushDataSync[PushDataResponse](pushDataMsg1)
    buf.retain()
    assert(res1.status == StatusCode.Success)
    res1 = worker1.pushDataSync[PushDataResponse](pushDataMsg1)
    buf.retain()
    assert(res1.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker1, shuffleKey)
    assert(getFileLengh(shuffleKey, worker1Location.getReduceId, worker1Location.getUUID) == 0)
    res1 = worker1.pushDataSync[PushDataResponse](pushDataMsg1)
    buf.retain()
    assert(res1.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker1, shuffleKey)
    assert(getFileLengh(shuffleKey, worker1Location.getReduceId, worker1Location.getUUID) == 128)
    res1 = worker1.pushDataSync[PushDataResponse](pushDataMsg1)
    buf.retain()
    assert(res1.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker1, shuffleKey)
    assert(getFileLengh(shuffleKey, worker1Location.getReduceId, worker1Location.getUUID) == 128)
    res1 = worker1.pushDataSync[PushDataResponse](pushDataMsg1)
    buf.retain()
    assert(res1.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker1, shuffleKey)
    assert(getFileLengh(shuffleKey, worker1Location.getReduceId, worker1Location.getUUID) == 256)

    val worker2Location = partitionLocations.filter(p => p.getPort == port2).toList(0)
    assert(getFileLengh(shuffleKey, worker2Location.getReduceId, worker2Location.getUUID) == 0)
    val pushDataMsg2 = new PushData(
      "appId-1",
      worker2Location.getUUID,
      PartitionLocation.Mode.Master.mode(),
      buf,
      TransportClient.requestId()
    )
    buf.retain()
    var res = worker2.pushDataSync[PushDataResponse](pushDataMsg2)
    assert(res.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    assert(getFileLengh(shuffleKey, worker2Location.getReduceId, worker2Location.getUUID) == 0)
    buf.retain()
    res = worker2.pushDataSync[PushDataResponse](pushDataMsg2)
    assert(res.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    assert(getFileLengh(shuffleKey, worker2Location.getReduceId, worker2Location.getUUID) == 0)
    buf.retain()
    res = worker2.pushDataSync[PushDataResponse](pushDataMsg2)
    assert(res.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    Thread.sleep(1000)
    assert(getFileLengh(shuffleKey, worker2Location.getReduceId, worker2Location.getUUID) == 128)
    buf.retain()
    res = worker2.pushDataSync[PushDataResponse](pushDataMsg2)
    assert(res.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    assert(getFileLengh(shuffleKey, worker2Location.getReduceId, worker2Location.getUUID) == 128)
    buf.retain()
    res = worker2.pushDataSync[PushDataResponse](pushDataMsg2)
    assert(res.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    assert(getFileLengh(shuffleKey, worker2Location.getReduceId, worker2Location.getUUID) == 256)

    val worker2Location2 = partitionLocations.filter(p => p.getPort == port2).toList(1)
    assert(getFileLengh(shuffleKey, worker2Location.getReduceId, worker2Location2.getUUID) == 0)
    val pushDataMsg3 = new PushData(
      "appId-1",
      worker2Location2.getUUID,
      PartitionLocation.Mode.Master.mode(),
      buf,
      TransportClient.requestId()
    )
    buf.retain()
    res = worker2.pushDataSync[PushDataResponse](pushDataMsg3)
    assert(res.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    assert(getFileLengh(shuffleKey, worker2Location2.getReduceId, worker2Location2.getUUID) == 0)
    buf.retain()
    res = worker2.pushDataSync[PushDataResponse](pushDataMsg3)
    assert(res.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    assert(getFileLengh(shuffleKey, worker2Location2.getReduceId, worker2Location2.getUUID) == 0)
    buf.retain()
    res = worker2.pushDataSync[PushDataResponse](pushDataMsg3)
    assert(res.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    assert(getFileLengh(shuffleKey, worker2Location2.getReduceId, worker2Location2.getUUID) == 128)
    buf.retain()
    res = worker2.pushDataSync[PushDataResponse](pushDataMsg3)
    assert(res.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    assert(getFileLengh(shuffleKey, worker2Location2.getReduceId, worker2Location2.getUUID) == 128)
    buf.retain()
    res = worker2.pushDataSync[PushDataResponse](pushDataMsg3)
    assert(res.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    assert(getFileLengh(shuffleKey, worker2Location2.getReduceId, worker2Location2.getUUID) == 256)

    val res2 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res2.status.equals(StatusCode.Success))
    val workerInfos = res2.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.length == 1)
    val worker = workerInfos.get(0)
    assert(worker.masterPartitionLocations.size() == 1)
    assert(worker.masterPartitionLocations.contains("appId-1"))
    assert(worker.masterPartitionLocations.get("appId-1").contains(worker2Location2))
    val doubleChunk = worker2.askSync[GetDoubleChunkInfoResponse](
      GetDoubleChunkInfo("appId-1", PartitionLocation.Mode.Master, worker2Location2)
    )
    assert(doubleChunk.working == 0)

    stop()
  }

  test("MapperEnd") {
    init()

    val resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(
        "appId",
        1,
        3,
        10
      )
    )
    val partitionLocations = resReg.partitionLocations

    val mapper1Locations = Random.shuffle(partitionLocations.asScala).take(3)
    val mapper2Locations = Random.shuffle(partitionLocations.asScala).take(4)
    val mapper3Locations = Random.shuffle(partitionLocations.asScala).take(5)

    var res = master.askSync[MapperEndResponse](
      MapperEnd(
        "appId",
        1,
        0,
        0,
        mapper1Locations
      )
    )
    assert(res.status.equals(StatusCode.Success))
    var groupRes = master.askSync[GetShuffleFileGroupResponse](
      GetShuffleFileGroup("appId", 1)
    )
    assert(groupRes.status.equals(StatusCode.Success))
    val orderKeys1: Array[String] = mapper1Locations.map(l => Utils.makePartitionKey("appId", 1, l.getReduceId)).sorted.toArray
    val orderValues1: Array[String] = mapper1Locations.map(l => EssPathUtil.GetPartitionPath(conf, "appId", 1, l.getReduceId, l.getUUID).toString).sorted.toArray
    assertResult(orderKeys1) {
      groupRes.fileGroup.keySet().asScala.toArray.sorted
    }
    assertResult(orderValues1) {
      groupRes.fileGroup.values().flatten.toArray.sorted
    }

    res = master.askSync[MapperEndResponse](
      MapperEnd(
        "appId",
        1,
        1,
        0,
        mapper2Locations
      )
    )
    assert(res.status.equals(StatusCode.Success))
    groupRes = master.askSync[GetShuffleFileGroupResponse](
      GetShuffleFileGroup("appId", 1)
    )
    assert(groupRes.status.equals(StatusCode.Success))
    val orderKeys2: Array[String] = (mapper1Locations ++ mapper2Locations)
      .distinct.map(l => Utils.makePartitionKey("appId", 1, l.getReduceId))
      .sorted.toArray
    val orderValues2: Array[String] = (mapper1Locations ++ mapper2Locations)
      .distinct.map(l => EssPathUtil.GetPartitionPath(conf, "appId", 1, l.getReduceId, l.getUUID).toString)
      .sorted.toArray
    assertResult(orderKeys2) {
      groupRes.fileGroup.keySet().asScala.toArray.sorted
    }
    assertResult(orderValues2) {
      groupRes.fileGroup.values().flatten.toArray.sorted
    }

    res = master.askSync[MapperEndResponse](
      MapperEnd(
        "appId",
        1,
        2,
        0,
        mapper3Locations
      )
    )
    assert(res.status.equals(StatusCode.Success))
    groupRes = master.askSync[GetShuffleFileGroupResponse](
      GetShuffleFileGroup("appId", 1)
    )
    assert(groupRes.status.equals(StatusCode.Success))
    val orderKeys3: Array[String] = (mapper1Locations ++ mapper2Locations ++ mapper3Locations)
      .distinct.map(l => Utils.makePartitionKey("appId", 1, l.getReduceId))
      .sorted.toArray
    val orderValues3: Array[String] = (mapper1Locations ++ mapper2Locations ++ mapper3Locations)
      .distinct.map(l => EssPathUtil.GetPartitionPath(conf, "appId", 1, l.getReduceId, l.getUUID).toString).sorted.toArray
    assertResult(orderKeys3) {
      groupRes.fileGroup.keySet().asScala.toArray.sorted
    }
    assertResult(orderValues3) {
      groupRes.fileGroup.values().flatten.toArray.sorted
    }

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

    assert(res.status.equals(StatusCode.Success))
    val partitionLocations = res.partitionLocations
    assert(partitionLocations.size() == 6)
    partitionLocations.foreach(p => {
      assert(p.getMode == PartitionLocation.Mode.Master)
      assert(p.getPeer != null)
      assert(p.getPeer.getMode == PartitionLocation.Mode.Slave)
    })

    var res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.status.equals(StatusCode.Success))
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
    assert(res2.status == StatusCode.Success)

    res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.status.equals(StatusCode.Success))
    workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.size() == 1)
    workerInfo = workerInfos.get(0)
    assert(workerInfo.masterPartitionLocations.get(shuffleKey) == null)
    assert(workerInfo.slavePartitionLocations.get(shuffleKey) == null)
    assert(workerInfo.memoryUsed == 0)

    res1 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.status.equals(StatusCode.Success))
    workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    workerInfo = workerInfos.get(0)
    assert(workerInfo.masterPartitionLocations.get(shuffleKey).size() == 3)
    assert(workerInfo.slavePartitionLocations.get(shuffleKey).size() == 3)
    res2 = worker2.askSync[DestroyResponse](
      Destroy(shuffleKey,
        workerInfo.masterPartitionLocations.get(shuffleKey).keySet().toList,
        workerInfo.slavePartitionLocations.get(shuffleKey).keySet().toList)
    )
    assert(res2.status == StatusCode.Success)

    res1 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.status.equals(StatusCode.Success))
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

    assert(res.status.equals(StatusCode.Success))
    val partitionLocations = res.partitionLocations
    assert(partitionLocations.size() == 6)
    partitionLocations.foreach(p => {
      assert(p.getMode == PartitionLocation.Mode.Master)
      assert(p.getPeer != null)
      assert(p.getPeer.getMode == PartitionLocation.Mode.Slave)
    })

    var res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.status.equals(StatusCode.Success))
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
    assert(res2.status == StatusCode.Success)

    res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.status.equals(StatusCode.Success))
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
    assert(res.status.equals(StatusCode.Success))

    // get partition info
    var res1 = worker1.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.status.equals(StatusCode.Success))
    assert(res1.workerInfos.asInstanceOf[util.List[WorkerInfo]].size() == 1)
    var workerInfo = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]].get(0)
    assert(workerInfo.memoryUsed == 128 * 10)
    assert(workerInfo.masterPartitionLocations.size() == 1)
    assert(workerInfo.slavePartitionLocations.size() == 1)
    assert(workerInfo.masterPartitionLocations.contains(shuffleKey))
    var masterLocations = workerInfo.masterPartitionLocations.get(shuffleKey)

    res1 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    assert(res1.status.equals(StatusCode.Success))
    workerInfo = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]].get(0)
    assert(workerInfo.memoryUsed == 128 * 10)
    var slaveLocations = workerInfo.slavePartitionLocations.get(shuffleKey)

    masterLocations.keySet().foreach(loc => assert(slaveLocations.contains(loc.getPeer)))

    // send data before trigger SlaveLost
    val lostSlave = masterLocations.head._1.getPeer
    0 until 5 foreach (_ => {
      val data = new Array[Byte](63)
      Random.nextBytes(data)
      val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
      worker1.pushDataSync[PushDataResponse](
        new PushData(
          shuffleKey,
          lostSlave.getPeer.getUUID,
          PartitionLocation.Mode.Master.mode(),
          buf,
          TransportClient.requestId()
        )
      )
    })

    // trigger slave lost
    val res2 = worker1.askSync[SlaveLostResponse](
      SlaveLost(shuffleKey, lostSlave.getPeer, lostSlave)
    )
    assert(res2.status == StatusCode.Success)
    waitUntilDataFinishFlushing(worker1, shuffleKey)
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

    assertChunkInfo(shuffleKey, List(masterLocation))
    masterLocation.setPeer(newPeer)
    assertChunkInfo(shuffleKey, List(masterLocation))

    val masterDoubleChunkInfo = worker1.askSync[GetDoubleChunkInfoResponse](
      GetDoubleChunkInfo(shuffleKey, PartitionLocation.Mode.Master, masterLocation)
    )
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

    locations.foreach(loc => {
      assert(loc.getPort != loc.getPeer.getPort)
    })

    0 until 5 foreach (_ => {
      locations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey, loc.getUUID, PartitionLocation.Mode.Master.mode,
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })

    val res = master.askSync[WorkerLostResponse](
      WorkerLost(locations.head.getHost, port1)
    )
    assert(res.success)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    Thread.sleep(1000)

    var res1 = master.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    var workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.size() == 1)
    var workerInfo = workerInfos.get(0)
    assert(!workerInfo.masterPartitionLocations.contains(shuffleKey))
    assert(!workerInfo.slavePartitionLocations.contains(shuffleKey))
    assert(workerInfo.memoryUsed == 0)

    res1 = worker2.askSync[GetWorkerInfosResponse](GetWorkerInfos)
    workerInfos = res1.workerInfos.asInstanceOf[util.List[WorkerInfo]]
    assert(workerInfos.size() == 1)
    workerInfo = workerInfos.get(0)
    assert(!workerInfo.masterPartitionLocations.contains(shuffleKey))
    assert(!workerInfo.slavePartitionLocations.contains(shuffleKey))
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
    assert(resReg.status.equals(StatusCode.Success))

    // check worker info
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
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = getWorker(loc).pushDataSync[PushDataResponse](
          new PushData(shuffleKey, loc.getUUID, PartitionLocation.Mode.Master.mode(),
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })

    assertInfos()

    // trigger worker lost
    master.askSync[WorkerLostResponse](
      WorkerLost(worker1.address.host, worker1.address.port)
    )
    Thread.sleep(2000)
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
    val (workerInfoMaster, workerInfoWorker) = if (worker1InfoMaster != null) {
      (worker1InfoMaster, worker1InfoWorker)
    } else if (worker2InfoMaster != null) {
      (worker2InfoMaster, worker2InfoWorker)
    } else {
      (worker3InfoMaster, worker3InfoWorker)
    }
    assert(workerInfoMaster.hasSameInfoWith(workerInfoWorker))
    assert(workerInfoMaster.masterPartitionLocations.size() == 0)
    assert(workerInfoMaster.slavePartitionLocations.size() == 0)

    stop(3)
  }

  test("StageEnd-NoWorkerLost") {
    init(3)

    val appId = "appId"
    val shuffleId = 1
    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
    // register shuffle
    val resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId, 12, 12)
    )

    // send data
    0 until 100 foreach (_ => {
      resReg.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey, loc.getUUID, PartitionLocation.Mode.Master.mode(),
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })

    waitUntilDataFinishFlushing(worker1, shuffleKey)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    waitUntilDataFinishFlushing(worker3, shuffleKey)

    assertInfos()
    assertChunkInfo(shuffleKey,
      worker1InfoMaster.masterPartitionLocations.get(shuffleKey).keySet().toList)
    assertChunkInfo(shuffleKey,
      worker2InfoMaster.masterPartitionLocations.get(shuffleKey).keySet().toList)
    assertChunkInfo(shuffleKey,
      worker3InfoMaster.masterPartitionLocations.get(shuffleKey).keySet().toList)

    // stage-end for shuffle-1
    val res = master.askSync[StageEndResponse](
      StageEnd(appId, shuffleId)
    )
    assert(res.status == StatusCode.Success)
    assertInfos()
    assert(worker1InfoMaster.masterPartitionLocations.size() == 0)
    assert(worker1InfoMaster.slavePartitionLocations.size() == 0)
    assert(worker2InfoMaster.masterPartitionLocations.size() == 0)
    assert(worker2InfoMaster.slavePartitionLocations.size() == 0)
    assert(worker3InfoMaster.masterPartitionLocations.size() == 0)
    assert(worker3InfoMaster.slavePartitionLocations.size() == 0)
    assert(worker1InfoMaster.memoryUsed == 0)
    assert(worker2InfoMaster.memoryUsed == 0)
    assert(worker3InfoMaster.memoryUsed == 0)
    assertFileLength(shuffleKey, resReg.partitionLocations, 63 * 100)

    /**
     * register shuffle-2
     */
    val shuffleId2 = 2
    val shuffleKey2 = Utils.makeShuffleKey(appId, shuffleId2)
    val resReg2 = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId2, 10, 15)
    )

    // send data
    0 until 50 foreach (_ => {
      resReg2.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey2, loc.getUUID, PartitionLocation.Mode.Master.mode,
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })

    waitUntilDataFinishFlushing(worker1, shuffleKey2)
    waitUntilDataFinishFlushing(worker2, shuffleKey2)
    waitUntilDataFinishFlushing(worker3, shuffleKey2)
    assertInfos()
    assertChunkInfo(shuffleKey2,
      worker1InfoWorker.masterPartitionLocations.get(shuffleKey2).keySet().toList)
    assertChunkInfo(shuffleKey2,
      worker2InfoWorker.masterPartitionLocations.get(shuffleKey2).keySet().toList)
    assertChunkInfo(shuffleKey2,
      worker3InfoWorker.masterPartitionLocations.get(shuffleKey2).keySet().toList)

    /**
     * register shuffle-3
     */
    val shuffleId3 = 3
    val shuffleKey3 = Utils.makeShuffleKey(appId, shuffleId3)
    val resReg3 = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId3, 20, 22)
    )

    // send data
    0 until 80 foreach (_ => {
      resReg3.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey3, loc.getUUID, PartitionLocation.Mode.Master.mode,
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })

    waitUntilDataFinishFlushing(worker1, shuffleKey3)
    waitUntilDataFinishFlushing(worker2, shuffleKey3)
    waitUntilDataFinishFlushing(worker3, shuffleKey3)
    assertInfos()
    assertChunkInfo(shuffleKey3,
      worker1InfoMaster.masterPartitionLocations.get(shuffleKey3).keySet().toList)
    assertChunkInfo(shuffleKey3,
      worker2InfoMaster.masterPartitionLocations.get(shuffleKey3).keySet().toList)
    assertChunkInfo(shuffleKey3,
      worker2InfoMaster.masterPartitionLocations.get(shuffleKey3).keySet().toList)

    /**
     * stage end for shuffle-3
     */
    val stageEndRes3 = master.askSync[StageEndResponse](
      StageEnd(appId, shuffleId3)
    )
    assert(stageEndRes3.status == StatusCode.Success)

    waitUntilDataFinishFlushing(worker1, shuffleKey3)
    waitUntilDataFinishFlushing(worker2, shuffleKey3)
    waitUntilDataFinishFlushing(worker3, shuffleKey3)
    assertInfos()
    assertChunkInfo(shuffleKey2,
      worker1InfoMaster.masterPartitionLocations.get(shuffleKey2).keySet().toList)
    assertChunkInfo(shuffleKey2,
      worker2InfoMaster.masterPartitionLocations.get(shuffleKey2).keySet().toList)
    assertChunkInfo(shuffleKey2,
      worker2InfoMaster.masterPartitionLocations.get(shuffleKey2).keySet().toList)
    assert(worker1InfoMaster.masterPartitionLocations.size() == 1)
    assert(worker1InfoMaster.masterPartitionLocations.contains(shuffleKey2))
    assert(worker1InfoMaster.masterPartitionLocations.get(shuffleKey2).size() == 5)
    assert(worker1InfoMaster.slavePartitionLocations.get(shuffleKey2).size() == 5)
    assertFileLength(shuffleKey3, resReg3.partitionLocations, 63 * 80)

    /**
     * stage end for shuffle-2
     */
    val stageEndRes2 = master.askSync[StageEndResponse](
      StageEnd(appId, shuffleId2)
    )
    assert(stageEndRes2.status == StatusCode.Success)

    waitUntilDataFinishFlushing(worker1, shuffleKey2)
    waitUntilDataFinishFlushing(worker2, shuffleKey2)
    waitUntilDataFinishFlushing(worker3, shuffleKey2)
    assertInfos()
    assert(worker1InfoMaster.masterPartitionLocations.size() == 0)
    assert(worker2InfoMaster.masterPartitionLocations.size() == 0)
    assert(worker3InfoMaster.masterPartitionLocations.size() == 0)
    assert(worker3InfoMaster.slavePartitionLocations.size() == 0)
    assertFileLength(shuffleKey2, resReg2.partitionLocations, 63 * 50)

    stop(3)
  }

  test("StageEnd-WithWorkerLost") {
    init(3)

    val appId = "appId"
    val shuffleId1 = 1
    val shuffleKey1 = Utils.makeShuffleKey(appId, shuffleId1)
    // register shuffle
    val resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId1, 10, 12)
    )
    assert(resReg.status.equals(StatusCode.Success))

    // send data
    0 until 100 foreach (_ => {
      resReg.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey1, loc.getUUID, PartitionLocation.Mode.Master.mode(),
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })

    waitUntilDataFinishFlushing(worker1, shuffleKey1)
    waitUntilDataFinishFlushing(worker2, shuffleKey1)
    waitUntilDataFinishFlushing(worker3, shuffleKey1)

    assertInfos()
    assertFileLength(shuffleKey1, resReg.partitionLocations, 63 * 98)

    // trigger worker lost
    val resWorkerLost = master.askSync[WorkerLostResponse](
      WorkerLost(worker1.address.host, worker1.address.port)
    )
    assert(resWorkerLost.success)
    Thread.sleep(100)

    assertFileLength(
      shuffleKey1,
      worker1InfoMaster.masterPartitionLocations.get(shuffleKey1).keySet().toList,
      63 * 100)
    assertFileLength(
      shuffleKey1,
      worker2InfoMaster.masterPartitionLocations.get(shuffleKey1).keySet().toList,
      63 * 98
    )
    assertFileLength(
      shuffleKey1,
      worker2InfoMaster.masterPartitionLocations.get(shuffleKey1).keySet().toList,
      63 * 98
    )

    waitUntilDataFinishFlushing(worker1, shuffleKey1)
    waitUntilDataFinishFlushing(worker2, shuffleKey1)
    waitUntilDataFinishFlushing(worker3, shuffleKey1)
    assertInfos()
    assert(worker2InfoMaster.masterPartitionLocations.size() == 1)
    assert(worker2InfoMaster.masterPartitionLocations.get(shuffleKey1).size() == 4)
    assert(worker2InfoMaster.slavePartitionLocations.get(shuffleKey1).size() == 4)
    assert(worker2InfoMaster.memoryUsed == 8 * 128)

    assert(worker3InfoMaster.masterPartitionLocations.size() == 1)
    assert(worker3InfoMaster.masterPartitionLocations.get(shuffleKey1).size() == 4)
    assert(worker3InfoMaster.slavePartitionLocations.get(shuffleKey1).size() == 4)
    assert(worker3InfoMaster.memoryUsed == 8 * 128)

    // trigger stage end for shuffle-1
    val resStageEnd = master.askSync[StageEndResponse](
      StageEnd(appId, shuffleId1)
    )
    assert(resStageEnd.status == StatusCode.Success)
    assertFileLength(shuffleKey1, resReg.partitionLocations, 63 * 100)
    assertInfos()
    assert(worker1InfoMaster == null)
    assert(worker2InfoMaster.memoryUsed == 0)
    assert(worker2InfoMaster.masterPartitionLocations.size() == 0)
    assert(worker3InfoMaster.memoryUsed == 0)
    assert(worker3InfoMaster.masterPartitionLocations.size() == 0)

    stop(3)
  }

  test("StageEnd-WithWorkerLost-3Stages") {
    init(3)

    val appId = "appId"

    // register shuffle-1
    val shuffleId1 = 1
    val shuffleKey1 = Utils.makeShuffleKey(appId, shuffleId1)
    val resReg1 = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId1, 10, 12)
    )
    assert(resReg1.status.equals(StatusCode.Success))
    // send data
    0 until 70 foreach (_ => {
      resReg1.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey1, loc.getUUID, PartitionLocation.Mode.Master.mode,
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })

    // register shuffle-2
    val shuffleId2 = 2
    val shuffleKey2 = Utils.makeShuffleKey(appId, shuffleId2)
    val resReg2 = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId2, 12, 14)
    )
    assert(resReg2.status.equals(StatusCode.Success))
    // send data
    0 until 90 foreach (_ => {
      resReg2.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey2, loc.getUUID, PartitionLocation.Mode.Master.mode,
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })

    // register shuffle-3
    val shuffleId3 = 3
    val shuffleKey3 = Utils.makeShuffleKey(appId, shuffleId3)
    val resReg3 = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId3, 12, 29)
    )
    assert(resReg3.status.equals(StatusCode.Success))
    // send data
    0 until 173 foreach (_ => {
      resReg3.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey3, loc.getUUID, PartitionLocation.Mode.Master.mode(),
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })

    waitUntilDataFinishFlushing(worker1, shuffleKey3)
    waitUntilDataFinishFlushing(worker2, shuffleKey3)
    waitUntilDataFinishFlushing(worker3, shuffleKey3)
    assertInfos()
    assertChunkInfo(shuffleKey1, resReg1.partitionLocations)
    assertChunkInfo(shuffleKey2, resReg2.partitionLocations)
    assertChunkInfo(shuffleKey3, resReg3.partitionLocations)

    assertFileLength(shuffleKey1, resReg1.partitionLocations, 63 * 68)
    assertFileLength(shuffleKey2, resReg2.partitionLocations, 63 * 88)
    assertFileLength(shuffleKey3, resReg3.partitionLocations, 63 * 172)

    // trigger WorkerLost
    val resWorkerLost = master.askSync[WorkerLostResponse](
      WorkerLost(worker2.address.host, worker2.address.port)
    )
    assert(resWorkerLost.success)
    assertFileLength(
      shuffleKey1,
      worker3InfoMaster.masterPartitionLocations.get(shuffleKey1).keySet().toList,
      63 * 68
    )
    assertFileLength(
      shuffleKey2,
      worker3InfoMaster.masterPartitionLocations.get(shuffleKey2).keySet().toList,
      63 * 88
    )
    assertFileLength(
      shuffleKey3,
      worker3InfoMaster.masterPartitionLocations.get(shuffleKey3).keySet().toList,
      63 * 172
    )

    assertFileLength(
      shuffleKey1,
      worker3InfoMaster.slavePartitionLocations.get(shuffleKey1).keySet().toList,
      63 * 70
    )
    assertFileLength(
      shuffleKey2,
      worker3InfoMaster.slavePartitionLocations.get(shuffleKey2).keySet().toList,
      63 * 90
    )
    assertFileLength(
      shuffleKey3,
      worker3InfoMaster.slavePartitionLocations.get(shuffleKey3).keySet().toList,
      63 * 173
    )

    waitUntilDataFinishFlushing(worker1, shuffleKey1)
    waitUntilDataFinishFlushing(worker2, shuffleKey1)
    waitUntilDataFinishFlushing(worker3, shuffleKey1)
    waitUntilDataFinishFlushing(worker1, shuffleKey2)
    waitUntilDataFinishFlushing(worker2, shuffleKey2)
    waitUntilDataFinishFlushing(worker3, shuffleKey2)
    waitUntilDataFinishFlushing(worker1, shuffleKey3)
    waitUntilDataFinishFlushing(worker2, shuffleKey3)
    waitUntilDataFinishFlushing(worker3, shuffleKey3)

    assertInfos()
    assertChunkInfo(shuffleKey1,
      worker1InfoWorker.masterPartitionLocations.get(shuffleKey1).keySet().toList)
    assertChunkInfo(shuffleKey2,
      worker1InfoWorker.masterPartitionLocations.get(shuffleKey2).keySet().toList)
    assertChunkInfo(shuffleKey3,
      worker1InfoWorker.masterPartitionLocations.get(shuffleKey3).keySet().toList)

    assertChunkInfo(shuffleKey1,
      worker3InfoWorker.masterPartitionLocations.get(shuffleKey1).keySet().toList)
    assertChunkInfo(shuffleKey2,
      worker3InfoWorker.masterPartitionLocations.get(shuffleKey2).keySet().toList)
    assertChunkInfo(shuffleKey3,
      worker3InfoWorker.masterPartitionLocations.get(shuffleKey3).keySet().toList)

    var stageEnd = master.askSync[StageEndResponse](
      StageEnd(appId, shuffleId1)
    )
    assert(stageEnd.status == StatusCode.Success)
    stageEnd = master.askSync[StageEndResponse](
      StageEnd(appId, shuffleId2)
    )
    assert(stageEnd.status == StatusCode.Success)
    stageEnd = master.askSync[StageEndResponse](
      StageEnd(appId, shuffleId3)
    )
    assert(stageEnd.status == StatusCode.Success)

    assertFileLength(shuffleKey1, resReg1.partitionLocations, 63 * 70)
    assertFileLength(shuffleKey2, resReg2.partitionLocations, 63 * 90)
    assertFileLength(shuffleKey3, resReg3.partitionLocations, 63 * 173)

    assertInfos()
    assert(worker1InfoMaster.memoryUsed == 0)
    assert(worker2InfoMaster == null)
    assert(worker3InfoMaster.memoryUsed == 0)

    stop(3)
  }

  test("Revive") {
    init(3)

    val appId = "appId"
    val shuffleId = 1
    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
    // register shuffle
    val resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId, 10, 12)
    )
    assert(resReg.status.equals(StatusCode.Success))

    // send data
    0 until 10 foreach (_ => {
      resReg.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey, loc.getUUID, PartitionLocation.Mode.Master.mode,
            buf, TransportClient.requestId())
        )
      })
    })

    waitUntilDataFinishFlushing(worker1, shuffleKey)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    waitUntilDataFinishFlushing(worker3, shuffleKey)
    assertInfos()

    // revive
    val resRev = master.askSync[ReviveResponse](
      Revive(appId, shuffleId, 0)
    )
    assert(resRev.status.equals(StatusCode.Success))
    val newLoc = resRev.partitionLocation
    // send data to new location
    val worker = getWorker(newLoc)
    val data = new Array[Byte](63)
    Random.nextBytes(data)
    val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
    0 until 1 foreach (_ => {
      val resSend = worker.pushDataSync[PushDataResponse](
        new PushData(shuffleKey, newLoc.getUUID, PartitionLocation.Mode.Master.mode(),
          buf, TransportClient.requestId())
      )
      assert(resSend.status == StatusCode.Success)
    })
    waitUntilDataFinishFlushing(worker, shuffleKey)

    assertInfos()

    // stage end
    val resStageEnd = master.askSync[StageEndResponse](
      StageEnd(appId, shuffleId)
    )
    assert(resStageEnd.status == StatusCode.Success)

    waitUntilDataFinishFlushing(worker1, shuffleKey)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    waitUntilDataFinishFlushing(worker3, shuffleKey)

    assertInfos()
    assertFileLength(shuffleKey, resReg.partitionLocations, 63 * 10)
    assertFileLength(shuffleKey, List(newLoc), 63 * 1)

    stop(3)
  }

  test("UnregisterShuffle") {
    init(3)

    // register shuffle
    val appId = "appId"
    val shuffleId = 1
    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
    val resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId, 10, 14)
    )
    assert(resReg.status.equals(StatusCode.Success))

    // send data
    0 until 10 foreach (_ => {
      resReg.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey, loc.getUUID, PartitionLocation.Mode.Master.mode(),
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })

    waitUntilDataFinishFlushing(worker1, shuffleKey)
    waitUntilDataFinishFlushing(worker2, shuffleKey)
    waitUntilDataFinishFlushing(worker3, shuffleKey)

    val shufflePath = EssPathUtil.GetShuffleDir(conf, appId, shuffleId)
    assert(fs.exists(shufflePath))

    // unregister shuffle without StageEnd
    var resUnreg = master.askSync[UnregisterShuffleResponse](
      UnregisterShuffle(appId, shuffleId)
    )
    assert(resUnreg.status == StatusCode.PartitionExists)

    // stage end
    val resStageEnd = master.askSync[StageEndResponse](
      StageEnd(appId, shuffleId)
    )
    assert(resStageEnd.status == StatusCode.Success)

    // unregister shuffle
    resUnreg = master.askSync[UnregisterShuffleResponse](
      UnregisterShuffle(appId, shuffleId)
    )
    assert(resUnreg.status == StatusCode.Success)
    assert(!fs.exists(shufflePath))

    stop(3)
  }

  test("ApplicationLost") {
    init(3)

    val appId = "appId"
    val shuffleId1 = 1
    val shuffleId2 = 2
    val shuffleId3 = 3
    val shuffleKey1 = Utils.makeShuffleKey(appId, shuffleId1)
    val shuffleKey2 = Utils.makeShuffleKey(appId, shuffleId2)
    val shuffleKey3 = Utils.makeShuffleKey(appId, shuffleId3)

    // register shuffle1 and send data
    var resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId1, 10, 12)
    )
    assert(resReg.status.equals(StatusCode.Success))
    0 until 9 foreach(_ => {
      resReg.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey1, loc.getUUID, PartitionLocation.Mode.Master.mode(),
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })
    val shufflePath1 = EssPathUtil.GetShuffleDir(conf, appId, shuffleId1)
    assert(fs.exists(shufflePath1))

    // register shuffle2 and send data
    resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId2, 16, 13)
    )
    assert(resReg.status.equals(StatusCode.Success))
    0 until 19 foreach(_ => {
      resReg.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey2, loc.getUUID, PartitionLocation.Mode.Master.mode(),
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })
    val shufflePath2 = EssPathUtil.GetShuffleDir(conf, appId, shuffleId2)
    assert(fs.exists(shufflePath2))

    // register shuffle3 and send data
    resReg = master.askSync[RegisterShuffleResponse](
      RegisterShuffle(appId, shuffleId3, 11, 33)
    )
    assert(resReg.status.equals(StatusCode.Success))
    0 until 31 foreach(_ => {
      resReg.partitionLocations.foreach(loc => {
        val worker = getWorker(loc)
        val data = new Array[Byte](63)
        Random.nextBytes(data)
        val buf = new NettyManagedBuffer(Unpooled.copiedBuffer(data))
        val res = worker.pushDataSync[PushDataResponse](
          new PushData(shuffleKey3, loc.getUUID, PartitionLocation.Mode.Master.mode(),
            buf, TransportClient.requestId())
        )
        assert(res.status == StatusCode.Success)
      })
    })
    val shufflePath3 = EssPathUtil.GetShuffleDir(conf, appId, shuffleId3)
    assert(fs.exists(shufflePath3))

    // wait for all data written
    waitUntilDataFinishFlushing(worker1, shuffleKey1)
    waitUntilDataFinishFlushing(worker2, shuffleKey1)
    waitUntilDataFinishFlushing(worker3, shuffleKey1)
    waitUntilDataFinishFlushing(worker1, shuffleKey2)
    waitUntilDataFinishFlushing(worker2, shuffleKey2)
    waitUntilDataFinishFlushing(worker3, shuffleKey2)
    waitUntilDataFinishFlushing(worker1, shuffleKey3)
    waitUntilDataFinishFlushing(worker2, shuffleKey3)
    waitUntilDataFinishFlushing(worker3, shuffleKey3)

    // trigger ApplicationLost
    val resAppLost = master.askSync[ApplicationLostResponse](
      ApplicationLost(appId)
    )
    assert(resAppLost.success)
    val path = EssPathUtil.GetAppDir(conf, appId)
    assert(!fs.exists(path))

    assertInfos()
    assert(worker1InfoMaster.memoryUsed == 0)
    assert(worker2InfoMaster.memoryUsed == 0)
    assert(worker3InfoMaster.memoryUsed == 0)

    stop(3)
  }

  test("ClientPushData") {
    init(3)

    val appId = "appId"
    val shuffleId1 = 1
    val shuffleKey1 = Utils.makeShuffleKey(appId, shuffleId1)
    val client = new ShuffleClientImpl()
    val res = client.registerShuffle(appId, shuffleId1, 10, 11)
    assert(res)

    0 until 114 foreach (_ => {
      0 until 11 foreach (reduceId => {
        val data = new Array[Byte](63)
        val buf = Unpooled.copiedBuffer(data)
        val res = client.pushData(appId, shuffleId1, reduceId, buf)
        assert(res)
      })
    })

    waitUntilDataFinishFlushing(worker1, shuffleKey1)
    waitUntilDataFinishFlushing(worker2, shuffleKey1)
    waitUntilDataFinishFlushing(worker3, shuffleKey1)
    assertInfos()

    // stageEnd
    val resStageEnd = client.stageEnd(appId, shuffleId1);
    assert(resStageEnd)
    waitUntilDataFinishFlushing(worker1, shuffleKey1)
    waitUntilDataFinishFlushing(worker2, shuffleKey1)
    waitUntilDataFinishFlushing(worker3, shuffleKey1)
    assertInfos()
    assert(worker1InfoMaster.memoryUsed == 0)
    assert(worker2InfoMaster.memoryUsed == 0)
    assert(worker3InfoMaster.memoryUsed == 0)
    val partitions = client.fetchShuffleInfo(appId, shuffleId1)
    assertFileLength(shuffleKey1, partitions, 63 * 114)

    // unregister shuffle
    val resUnreg = client.unregisterShuffle(appId, shuffleId1)
    assert(resUnreg)
    assertInfos()
    assert(worker1InfoMaster.memoryUsed == 0)
    assert(worker2InfoMaster.memoryUsed == 0)
    assert(worker3InfoMaster.memoryUsed == 0)

    client.shutDown()
    stop(3)
  }
}