package com.aliyun.emr.jss.service.deploy.worker

import java.util

import com.aliyun.emr.jss.common.rpc.RpcEndpointRef
import com.aliyun.emr.jss.common.util.Utils
import com.aliyun.emr.jss.protocol.PartitionLocation

import scala.collection.JavaConversions._

private[jss] class WorkerInfo(
  val host: String,
  val port: Int,
  val memory: Long,
  val partitionSize: Long,
  val endpoint: RpcEndpointRef) extends Serializable {

  Utils.checkHost(host)
  assert(port > 0)

  var memoryUsed: Long = _
  var lastHeartbeat: Long = _
  // key: shuffleKey  value: master locations
  val masterPartitionLocations =
    new util.HashMap[String, util.Map[PartitionLocation, PartitionLocation]]()
  // key: shuffleKey  value: slave locations
  val slavePartitionLocations =
    new util.HashMap[String, util.Map[PartitionLocation, PartitionLocation]]()

  init()

  // only exposed for test
  def freeMemory: Long = memory - memoryUsed

  def slotAvailable(): Boolean = freeMemory >= partitionSize

  private def init() {
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  def addMasterPartition(shuffleKey: String, location: PartitionLocation): Unit = {
    masterPartitionLocations.putIfAbsent(shuffleKey,
      new util.HashMap[PartitionLocation, PartitionLocation]())
    val masterLocs = masterPartitionLocations.get(shuffleKey)
    masterLocs.put(location, location)
    memoryUsed += partitionSize
  }

  def addMasterPartition(shuffleKey: String, locations: util.List[PartitionLocation]): Unit = {
    masterPartitionLocations.putIfAbsent(shuffleKey,
      new util.HashMap[PartitionLocation, PartitionLocation]())
    val masterLocs = masterPartitionLocations.get(shuffleKey)
    locations.foreach(loc => masterLocs.put(loc, loc))
    memoryUsed += partitionSize * locations.size()
  }

  def addSlavePartition(shuffleKey: String, location: PartitionLocation): Unit = {
    slavePartitionLocations.putIfAbsent(shuffleKey,
      new util.HashMap[PartitionLocation, PartitionLocation]())
    val slaveLocs = slavePartitionLocations.get(shuffleKey)
    slaveLocs.put(location, location)
    memoryUsed += partitionSize
  }

  def addSlavePartition(shuffleKey: String, locations: util.List[PartitionLocation]): Unit = {
    slavePartitionLocations.putIfAbsent(shuffleKey,
      new util.HashMap[PartitionLocation, PartitionLocation]())
    val slaveLocs = slavePartitionLocations.get(shuffleKey)
    locations.foreach(loc => slaveLocs.put(loc,loc))
    memoryUsed += partitionSize * locations.size()
  }

  def removeMasterPartition(shuffleKey: String, location: PartitionLocation): Unit = {
    if (!masterPartitionLocations.containsKey(shuffleKey)) {
      return
    }
    val masterLocs = masterPartitionLocations.get(shuffleKey)
    val removed = masterLocs.remove(location)
    if (removed != null) {
      memoryUsed -= partitionSize
    }
  }

  def removeMasterPartition(shuffleKey: String, locations: util.List[PartitionLocation]): Unit = {
    if (!masterPartitionLocations.containsKey(shuffleKey)) {
      return
    }
    val masterLocs = masterPartitionLocations.get(shuffleKey)
    locations.foreach(loc => {
      val removed = masterLocs.remove(loc)
      if (removed != null) {
        memoryUsed -= partitionSize
      }
    })
  }

  def removeSlavePartition(shuffleKey: String, location: PartitionLocation): Unit = {
    if (!slavePartitionLocations.containsKey(shuffleKey)) {
      return
    }
    val slaveLocs = slavePartitionLocations.get(shuffleKey)
    val removed = slaveLocs.remove(location)
    if (removed != null) {
      memoryUsed -= partitionSize
    }
  }

  def removeSlavePartition(shuffleKey: String, locations: util.List[PartitionLocation]): Unit = {
    if (!slavePartitionLocations.containsKey(shuffleKey)) {
      return
    }
    val slaveLocs = slavePartitionLocations.get(shuffleKey)
    locations.foreach(loc => {
      val removed = slaveLocs.remove(loc)
      if (removed != null) {
        memoryUsed -= partitionSize
      }
    })
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
