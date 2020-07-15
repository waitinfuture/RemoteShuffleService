package com.aliyun.emr.ess.service.deploy.worker

import java.lang.management.ManagementFactory
import java.util

import com.aliyun.emr.ess.common.rpc.RpcEndpointRef
import com.aliyun.emr.ess.common.util.Utils
import com.aliyun.emr.ess.protocol.PartitionLocation
import scala.collection.JavaConversions._

import com.aliyun.emr.ess.common.internal.Logging

private[ess] class WorkerInfo(
  val host: String,
  val port: Int,
  val memory: Long,
  val partitionSize: Long,
  val endpoint: RpcEndpointRef)
  extends Serializable with Logging {

  Utils.checkHost(host)
  assert(port > 0)

  var memoryUsed: Long = _
  var lastHeartbeat: Long = _
  // key: shuffleKey  value: (reduceId, master locations)
  type PartitionInfo = util.HashMap[String, util.Map[Int, util.List[PartitionLocation]]]
  val masterPartitionLocations =
    new PartitionInfo()
  val slavePartitionLocations =
    new PartitionInfo()

  init()

  // only exposed for test
  def freeMemory: Long = memory - memoryUsed

  def slotAvailable(): Boolean = freeMemory >= partitionSize

  private def init() {
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert(port > 0)
    host + ":" + port
  }

  def addPartition(shuffleKey: String,
    location: PartitionLocation,
    partitionInfo: PartitionInfo): Unit = {
    partitionInfo.putIfAbsent(shuffleKey,
      new util.HashMap[Int, util.List[PartitionLocation]]())
    val reduceLocMap = partitionInfo.get(shuffleKey)
    reduceLocMap.putIfAbsent(location.getReduceId, new util.ArrayList[PartitionLocation]())
    val locs = reduceLocMap.get(location.getReduceId)
    locs.add(location)
    memoryUsed += partitionSize
  }

  private def addPartition(shuffleKey: String,
    locations: util.List[PartitionLocation],
    partitionInfo: PartitionInfo): Unit = {
    partitionInfo.putIfAbsent(shuffleKey,
      new util.HashMap[Int, util.List[PartitionLocation]]())
    val reduceLocMap = partitionInfo.get(shuffleKey)
    locations.foreach(loc => {
      reduceLocMap.putIfAbsent(loc.getReduceId, new util.ArrayList[PartitionLocation]())
      val locs = reduceLocMap.get(loc.getReduceId)
      locs.add(loc)
    })
    memoryUsed += partitionSize * locations.size()
  }

  def addMasterPartition(shuffleKey: String, location: PartitionLocation): Unit = {
    addPartition(shuffleKey, location, masterPartitionLocations)
  }

  def addMasterPartition(shuffleKey: String, locations: util.List[PartitionLocation]): Unit = {
    addPartition(shuffleKey, locations, masterPartitionLocations)
  }

  def addSlavePartition(shuffleKey: String, location: PartitionLocation): Unit = {
    addPartition(shuffleKey, location, slavePartitionLocations)
  }

  def addSlavePartition(shuffleKey: String, locations: util.List[PartitionLocation]): Unit = {
    addPartition(shuffleKey, locations, slavePartitionLocations)
  }

  def removePartition(shuffleKey: String,
    uniqueId: String,
    partitionInfo: PartitionInfo
  ): Unit = {
    if (!partitionInfo.containsKey(shuffleKey)) {
      return
    }
    val tokens = uniqueId.split("-", 2)
    val reduceId = tokens(0).toInt
    val epoch = tokens(1).toInt
    val reduceLocMap = partitionInfo.get(shuffleKey)
    val locs = reduceLocMap.get(reduceId)
    if (locs != null) {
      val res = locs.find(_.getEpoch == epoch).orNull
      if (res != null) {
        locs.remove(res)
        memoryUsed -= partitionSize
      }
      if (locs.size() == 0) {
        reduceLocMap.remove(reduceId)
      }
    }

    if (reduceLocMap.size() == 0) {
      partitionInfo.remove(shuffleKey)
    }
  }

  def removePartition(shuffleKey: String,
    uniqueIds: util.Collection[String],
    partitionInfo: PartitionInfo): Unit = {
    if (!partitionInfo.containsKey(shuffleKey)) {
      return
    }
    val reduceLocMap = partitionInfo.get(shuffleKey)
    uniqueIds.foreach(id => {
      val tokens = id.split("-", 2)
      val reduceId = tokens(0).toInt
      val epoch = tokens(1).toInt
      val locs = reduceLocMap.get(reduceId)
      if (locs != null) {
        val res = locs.find(_.getEpoch == epoch).orNull
        if (res != null) {
          locs.remove(res)
          memoryUsed -= partitionSize
        }
        if (locs.size() == 0) {
          reduceLocMap.remove(reduceId)
        }
      }
    })

    if (reduceLocMap.size() == 0) {
      partitionInfo.remove(shuffleKey)
    }
  }

  def removeMasterPartition(shuffleKey: String, uniqueId: String): Unit = {
    removePartition(shuffleKey, uniqueId, masterPartitionLocations)
  }

  def removeMasterPartition(shuffleKey: String, uniqueIds: util.Collection[String]): Unit = {
    removePartition(shuffleKey, uniqueIds, masterPartitionLocations)
  }

  def removeMasterPartition(shuffleKey: String): Unit = {
    val uniqueIds = getAllMasterIds(shuffleKey)
    removeMasterPartition(shuffleKey, uniqueIds)
    masterPartitionLocations.remove(shuffleKey)
  }

  def removeSlavePartition(shuffleKey: String, uniqueId: String): Unit = {
    removePartition(shuffleKey, uniqueId, slavePartitionLocations)
  }

  def removeSlavePartition(shuffleKey: String,
    uniqueIds: util.Collection[String]): Unit = {
    removePartition(shuffleKey, uniqueIds, slavePartitionLocations)
  }

  def removeSlavePartition(shuffleKey: String): Unit = {
    val uniqueIds = getAllSlaveIds(shuffleKey)
    removeSlavePartition(shuffleKey, uniqueIds)
    slavePartitionLocations.remove(shuffleKey)
  }

  def getAllIds(shuffleKey: String, partitionInfo: PartitionInfo): util.List[String] = {
    if (!partitionInfo.containsKey(shuffleKey)) {
      return null
    }
    new util.ArrayList(
      partitionInfo.get(shuffleKey)
        .values()
        .flatMap(l => l)
        .map(_.getUniqueId)
    )
  }

  def getAllMasterIds(shuffleKey: String): util.List[String] = {
    getAllIds(shuffleKey, masterPartitionLocations)
  }

  def getAllSlaveIds(shuffleKey: String): util.List[String] = {
    getAllIds(shuffleKey, slavePartitionLocations)
  }

  def getAllMasterLocations(shuffleKey: String): util.List[PartitionLocation] = {
    if (masterPartitionLocations.containsKey(shuffleKey)) {
      new util.ArrayList[PartitionLocation](
        masterPartitionLocations.get(shuffleKey)
          .values()
          .flatMap(l => l)
      )
    } else new util.ArrayList[PartitionLocation]()
  }

  def getAllSlaveLocations(shuffleKey: String): util.List[PartitionLocation] = {
    if (slavePartitionLocations.containsKey(shuffleKey)) {
      new util.ArrayList[PartitionLocation](
        slavePartitionLocations.get(shuffleKey)
          .values()
          .flatMap(l => l)
      )
    } else new util.ArrayList[PartitionLocation]()
  }

  def setMasterPeer(shuffleKey: String, loc: PartitionLocation, peer: PartitionLocation): Unit = {
    masterPartitionLocations.get(shuffleKey).get(loc.getReduceId)
      .find(l => l.getEpoch == loc.getEpoch).get
      .setPeer(peer)
  }

  def getLocation(shuffleKey: String, uniqueId: String,
    mode: PartitionLocation.Mode): PartitionLocation = {
    val tokens = uniqueId.split("-", 2)
    val reduceId = tokens(0).toInt
    val epoch = tokens(1).toInt
    val partitionInfo = if (mode == PartitionLocation.Mode.Master) {
      masterPartitionLocations
    } else slavePartitionLocations

    if (!partitionInfo.containsKey(shuffleKey)
      || !partitionInfo.get(shuffleKey).containsKey(reduceId)) {
      return null
    }
    partitionInfo.get(shuffleKey)
      .get(reduceId)
      .find(loc => loc.getEpoch == epoch).orNull
  }

  def getMasterLocation(shuffleKey: String, uniqueId: String): PartitionLocation = {
    getLocation(shuffleKey, uniqueId, PartitionLocation.Mode.Master)
  }

  def getSlaveLocation(shuffleKey: String, uniqueId: String): PartitionLocation = {
    getLocation(shuffleKey, uniqueId, PartitionLocation.Mode.Slave)
  }

  def getLocationWithMaxEpoch(shuffleKey: String, reduceId: Int): Option[PartitionLocation] = {
    if (!masterPartitionLocations.containsKey(shuffleKey) ||
      !masterPartitionLocations.get(shuffleKey).containsKey(reduceId)) {
      return None
    }
    val locs = masterPartitionLocations.get(shuffleKey).get(reduceId)
    if (locs == null || locs.size() == 0) {
      return None
    }
    var curentEpoch = -1
    var currentPartition: PartitionLocation = null
    locs.foreach(loc => {
      if (loc.getEpoch > curentEpoch) {
        curentEpoch = loc.getEpoch
        currentPartition = loc
      }
    })
    Some(currentPartition)
  }

  def containsShuffleMaster(shuffleKey: String): Boolean = {
    masterPartitionLocations.containsKey(shuffleKey)
  }

  def containsShuffleSlave(shuffleKey: String): Boolean = {
    slavePartitionLocations.containsKey(shuffleKey)
  }

  def getMasterShuffleKeys(): util.Set[String] = {
    masterPartitionLocations.keySet()
  }

  def getSlaveShuffleKeys(): util.Set[String] = {
    slavePartitionLocations.keySet()
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

  override def toString(): String = {
    s"""
       |Address: ${hostPort}
       |Capacity: ${memory}
       |MemoryUsed: ${memoryUsed}
       |PartitionBufferSize: ${partitionSize}
       |SlotsUsed: ${memoryUsed / partitionSize}
       |SlotsAvailable: ${freeMemory / partitionSize}
       |MasterLocations: ${masterPartitionLocations.size()}
       |SlaveLocations: ${slavePartitionLocations.size()}
       |""".stripMargin
  }

  override def equals(obj: Any): Boolean = {
    val other = obj.asInstanceOf[WorkerInfo]
    host == other.host && port == other.port
  }

  override def hashCode(): Int = {
    hostPort.hashCode
  }
}

object WorkerInfo {
  def main(args: Array[String]): Unit = {
    val runtimeMXBean = ManagementFactory.getRuntimeMXBean
    println(runtimeMXBean.getName)
    val pid = runtimeMXBean.getName.split("@")(0)
    println(pid)

    val stream = Runtime.getRuntime.exec(s"jstack -l ${pid}").getInputStream
    val sb = new StringBuilder
    var res = stream.read()
    while (res != -1) {
      sb.append(res.toChar)
      res = stream.read()
    }
    println(sb)
  }
}
