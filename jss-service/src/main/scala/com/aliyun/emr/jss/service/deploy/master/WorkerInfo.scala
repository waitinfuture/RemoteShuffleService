package com.aliyun.emr.jss.service.deploy.master

import java.util

import com.aliyun.emr.jss.common.rpc.RpcEndpointRef
import com.aliyun.emr.jss.common.util.Utils

private[jss] class WorkerInfo(
  val host: String,
  val port: Int,
  val memory: Long,
  val partitionSize: Int,
  val endpoint: RpcEndpointRef) extends Serializable {

  Utils.checkHost(host)
  assert(port > 0)

  var memoryUsed: Long = _
  var lastHeartbeat: Long = _
  // stores PartitionLocation's UUID
  val masterPartitionLocations = new util.ArrayList[String]()
  val slavePartitionLocations = new util.ArrayList[String]()

  init()

  // only exposed for test
  def freeMemory: Long = memory - memoryUsed

  def slotAvailable(): Boolean = freeMemory >= partitionSize

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  def addMasterPartition(partitionId: String): Unit = {
    masterPartitionLocations.add(partitionId)
    memoryUsed += partitionSize
  }

  def addMasterPartition(partitionIds: util.List[String]): Unit = {
    masterPartitionLocations.addAll(partitionIds)
    memoryUsed += partitionSize * partitionIds.size()
  }

  def addSlavePartition(partitionId: String): Unit = {
    slavePartitionLocations.add(partitionId)
    memoryUsed += partitionSize
  }

  def addSlavePartition(partitionIds: util.List[String]): Unit = {
    slavePartitionLocations.addAll(partitionIds)
    memoryUsed += partitionSize * partitionIds.size()
  }

  def removeMasterPartition(partitionLocationId: String): Unit = {
    masterPartitionLocations.remove(partitionLocationId)
    memoryUsed -= partitionSize
  }

  def removeSlavePartition(partitionLocationId: String): Unit = {
    slavePartitionLocations.remove(partitionLocationId)
    memoryUsed -= partitionSize
  }

  def clearAll(): Unit = {
    memoryUsed = 0
    masterPartitionLocations.clear()
    slavePartitionLocations.clear()
  }

  override def equals(obj: Any): Boolean = {
    val other = obj.asInstanceOf[WorkerInfo]
    host == other.host && port == other.port
  }

  override def hashCode(): Int = {
    hostPort.hashCode
  }

  override def toString: String = {
    s"${host}:${port}  ${freeMemory}/${memory}"
  }
}
