package com.aliyun.emr.jss.service.deploy.master

import java.util

import com.aliyun.emr.jss.common.rpc.RpcEndpointRef
import com.aliyun.emr.jss.common.util.Utils

private[jss] class WorkerInfo(
    val id: String,
    val host: String,
    val port: Int,
    val memory: Int,
    val endpoint: RpcEndpointRef) extends Serializable with Comparable[WorkerInfo] {

  Utils.checkHost(host)
  assert(port > 0)

  var state: WorkerState.Value = _
  var memoryUsed: Int = _
  var lastHeartbeat: Long = _
  // stores PartitionLocation's UUID
  val masterPartitionLocations = new util.ArrayList[String]()
  val slavePartitionLocations = new util.ArrayList[String]()

  init()

  def freeMemory: Int = memory - memoryUsed

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    state = WorkerState.ALIVE
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  def addMasterPartition(partitionLocationId: String, partitionMemory: Int) {
    masterPartitionLocations.add(partitionLocationId)
    memoryUsed += partitionMemory
  }

  def addSlavePartition(partitionLocationId: String, partitionMemory: Int): Unit = {
    slavePartitionLocations.add(partitionLocationId)
    memoryUsed += partitionMemory
  }

  def removeMasterPartition(partitionLocationId: String, partitionMemory: Int) {
    masterPartitionLocations.remove(partitionLocationId)
    memoryUsed -= partitionMemory
  }

  def removeSlavePartition(partitionLocationId: String, partitionMemory: Int): Unit = {
    slavePartitionLocations.remove(partitionLocationId)
    memoryUsed -= partitionMemory
  }

  def clearAll(): Unit = {
    memoryUsed = 0
    masterPartitionLocations.clear()
    slavePartitionLocations.clear()
  }

  def setState(state: WorkerState.Value): Unit = {
    this.state = state
  }

  def isAlive(): Boolean = this.state == WorkerState.ALIVE

  override def compareTo(o: WorkerInfo): Int = {
    if (o.id == this.id) {
      0
    } else if (o.id > this.id) {
      -1
    } else {
      1
    }
  }

  override def equals(obj: Any): Boolean = {
    val other = obj.asInstanceOf[WorkerInfo]
    host == other.host && port == other.port
  }

  override def hashCode(): Int = {
    hostPort.hashCode
  }
}
