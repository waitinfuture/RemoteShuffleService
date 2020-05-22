package com.aliyun.emr.jss.service.deploy

import java.util

import scala.collection.JavaConversions._

import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEnv}
import com.aliyun.emr.jss.common.util.Utils
import com.aliyun.emr.jss.common.EssConf
import com.aliyun.emr.jss.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.jss.protocol.message.ControlMessages._
import com.aliyun.emr.jss.service.deploy.master.{Master, MasterArguments}
import com.aliyun.emr.jss.service.deploy.worker.{Worker, WorkerArguments}
import org.scalatest.FunSuite

class MessageHandlerSuite extends FunSuite {
  /**
   * ===============================
   *         start master
   * ===============================
   */
  val conf = new EssConf()
  val masterArgs = new MasterArguments(Array.empty[String], conf)
  val rpcEnvMaster = RpcEnv.create(
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
  val rpcEnvWorker1 = RpcEnv.create(RpcNameConstants.WORKER_SYS,
    workerArgs.host,
    workerArgs.port,
    conf)

  val masterAddresses = RpcAddress.fromJindoURL(workerArgs.master)
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
  val rpcEnvWorker2 = RpcEnv.create(RpcNameConstants.WORKER_SYS,
    workerArgs2.host,
    workerArgs2.port,
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
    new EssConf()
  )
  val master = env.setupEndpointRef(new RpcAddress(localhost, 9099), RpcNameConstants.MASTER_EP)

  /**
   * ===============================
   *         start testing
   * ===============================
   */
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
    val partitionLocations = res.partitionLocations
    assert(partitionLocations.size() == 10)
    partitionLocations.foreach(p => {
      assert(p.getMode == PartitionLocation.Mode.Master)
      assert(p.getPeer != null)
      assert(p.getPeer.getMode == PartitionLocation.Mode.Slave)
    })
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
      SlaveLost("appId-1", slaveLocation)
    )
    println(res)
  }
}
