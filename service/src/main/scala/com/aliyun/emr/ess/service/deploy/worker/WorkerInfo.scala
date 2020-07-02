package com.aliyun.emr.ess.service.deploy.worker

import java.util

import com.aliyun.emr.ess.common.rpc.RpcEndpointRef
import com.aliyun.emr.ess.common.util.Utils
import com.aliyun.emr.ess.protocol.PartitionLocation

import scala.collection.JavaConversions._

private[ess] class WorkerInfo(
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
    new util.HashMap[String, util.Map[String, PartitionLocation]]()
  // key: shuffleKey  value: slave locations
  val slavePartitionLocations =
    new util.HashMap[String, util.Map[String, PartitionLocation]]()

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
      new util.HashMap[String, PartitionLocation]())
    val masterLocs = masterPartitionLocations.get(shuffleKey)
    masterLocs.put(location.getUUID, location)
    memoryUsed += partitionSize
  }

  def addMasterPartition(shuffleKey: String, locations: util.List[PartitionLocation]): Unit = {
    masterPartitionLocations.putIfAbsent(shuffleKey,
      new util.HashMap[String, PartitionLocation]())
    val masterLocs = masterPartitionLocations.get(shuffleKey)
    locations.foreach(loc => masterLocs.put(loc.getUUID, loc))
    memoryUsed += partitionSize * locations.size()
  }

  def addSlavePartition(shuffleKey: String, location: PartitionLocation): Unit = {
    slavePartitionLocations.putIfAbsent(shuffleKey,
      new util.HashMap[String, PartitionLocation]())
    val slaveLocs = slavePartitionLocations.get(shuffleKey)
    slaveLocs.put(location.getUUID, location)
    memoryUsed += partitionSize
  }

  def addSlavePartition(shuffleKey: String, locations: util.List[PartitionLocation]): Unit = {
    slavePartitionLocations.putIfAbsent(shuffleKey,
      new util.HashMap[String, PartitionLocation]())
    val slaveLocs = slavePartitionLocations.get(shuffleKey)
    locations.foreach(loc => slaveLocs.put(loc.getUUID, loc))
    memoryUsed += partitionSize * locations.size()
  }

  def removeMasterPartition(shuffleKey: String, id: String): Unit = {
    if (!masterPartitionLocations.containsKey(shuffleKey)) {
      return
    }
    val masterLocs = masterPartitionLocations.get(shuffleKey)
    val removed = masterLocs.remove(id)
    if (removed != null) {
      memoryUsed -= partitionSize
    }
    if (masterLocs.size() == 0) {
      masterPartitionLocations.remove(shuffleKey)
    }
  }

  def removeMasterPartition(shuffleKey: String, ids: util.Collection[String]): Unit = {
    if (!masterPartitionLocations.containsKey(shuffleKey)) {
      return
    }
    val masterLocs = masterPartitionLocations.get(shuffleKey)
    ids.foreach(id => {
      val removed = masterLocs.remove(id)
      if (removed != null) {
        memoryUsed -= partitionSize
      }
    })
    if (masterLocs.size() == 0) {
      masterPartitionLocations.remove(shuffleKey)
    }
  }

  def removeSlavePartition(shuffleKey: String, id: String): Unit = {
    if (!slavePartitionLocations.containsKey(shuffleKey)) {
      return
    }
    val slaveLocs = slavePartitionLocations.get(shuffleKey)
    val removed = slaveLocs.remove(id)
    if (removed != null) {
      memoryUsed -= partitionSize
    }
    if (slaveLocs.size() == 0) {
      slavePartitionLocations.remove(shuffleKey)
    }
  }

  def removeSlavePartition(shuffleKey: String,
    locations: util.Collection[String]): Unit = {
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
    if (slaveLocs.size() == 0) {
      slavePartitionLocations.remove(shuffleKey)
    }
  }

  def clearAll(): Unit = {
    memoryUsed = 0
    masterPartitionLocations.clear()
    slavePartitionLocations.clear()
  }

  def hasSameInfoWith(other: WorkerInfo): Boolean = {
    memory == other.memory &&
    memoryUsed == other.memoryUsed &&
    hostPort == other.hostPort &&
    partitionSize == other.partitionSize &&
    masterPartitionLocations.size() == other.masterPartitionLocations.size() &&
    masterPartitionLocations.keySet().forall(key => {
      other.masterPartitionLocations.keySet().contains(key)
    }) &&
    masterPartitionLocations.forall(entry => {
      val shuffleKey = entry._1
      val masters = entry._2
      val otherMasters = other.masterPartitionLocations.get(shuffleKey)
      masters.forall(loc => otherMasters.contains(loc._1))
    }) &&
    slavePartitionLocations.size() == other.slavePartitionLocations.size() &&
    slavePartitionLocations.keySet().forall((key => {
      other.slavePartitionLocations.keySet().contains(key)
    })) &&
    slavePartitionLocations.forall(entry => {
      val shuffleKey = entry._1
      val slaves = entry._2
      val otherSlaves = other.slavePartitionLocations.get(shuffleKey)
      slaves.forall(loc => otherSlaves.contains(loc._1))
    })

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
