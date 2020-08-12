package com.aliyun.emr.ess.service.deploy.worker

import scala.collection.JavaConversions._

import java.util

import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.rpc.RpcEndpointRef
import com.aliyun.emr.ess.common.rpc.netty.NettyRpcEndpointRef
import com.aliyun.emr.ess.common.util.Utils
import com.aliyun.emr.ess.protocol.PartitionLocation

private[ess] class WorkerInfo(
    val host: String,
    val port: Int,
    val fetchPort: Int,
    val numSlots: Int,
    val endpoint: RpcEndpointRef)
  extends Serializable with Logging {

  Utils.checkHost(host)
  assert(port > 0)

  private var slotsUsed: Int = 0
  var lastHeartbeat: Long = _

  // key: shuffleKey  value: (reduceId, master locations)
  type PartitionInfo = util.HashMap[String, util.Map[Int, util.List[PartitionLocation]]]
  private val masterPartitionLocations =
    new PartitionInfo()
  private val slavePartitionLocations =
    new PartitionInfo()

  init()

  def isActive(): Boolean = {
    endpoint.asInstanceOf[NettyRpcEndpointRef].client.isActive
  }

  // only exposed for test
  def freeSlots(): Long = this.synchronized {
    numSlots - slotsUsed
  }

  def slotAvailable(): Boolean = this.synchronized {
    numSlots > slotsUsed
  }

  private def init() {
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert(port > 0)
    host + ":" + port
  }

  def shuffleKeySet(): util.HashSet[String] = {
    val shuffleKeySet = new util.HashSet[String]
    this.synchronized {
      shuffleKeySet.addAll(masterPartitionLocations.keySet())
      shuffleKeySet.addAll(slavePartitionLocations.keySet())
    }
    shuffleKeySet
  }

  private def addPartition(
      shuffleKey: String,
      location: PartitionLocation,
      partitionInfo: PartitionInfo): Unit = this.synchronized {
    if (location != null) {
      partitionInfo.putIfAbsent(shuffleKey,
        new util.HashMap[Int, util.List[PartitionLocation]]())
      val reduceLocMap = partitionInfo.get(shuffleKey)
      reduceLocMap.putIfAbsent(location.getReduceId, new util.ArrayList[PartitionLocation]())
      val locations = reduceLocMap.get(location.getReduceId)
      locations.add(location)
      slotsUsed += 1
    }
  }

  private def addPartitions(
      shuffleKey: String,
      locations: util.List[PartitionLocation],
      partitionInfo: PartitionInfo): Unit = this.synchronized {
    if (locations != null && locations.size() > 0) {
      partitionInfo.putIfAbsent(shuffleKey,
        new util.HashMap[Int, util.List[PartitionLocation]]())
      val reduceLocMap = partitionInfo.get(shuffleKey)
      locations.foreach { loc =>
        reduceLocMap.putIfAbsent(loc.getReduceId, new util.ArrayList[PartitionLocation]())
        val locations = reduceLocMap.get(loc.getReduceId)
        locations.add(loc)
      }
      slotsUsed += locations.size()
    }
  }

  private def removePartition(
      shuffleKey: String,
      uniqueId: String,
      partitionInfo: PartitionInfo): Unit = this.synchronized {
    if (!partitionInfo.containsKey(shuffleKey)) {
      return
    }
    val tokens = uniqueId.split("-", 2)
    val reduceId = tokens(0).toInt
    val epoch = tokens(1).toInt
    val reduceLocMap = partitionInfo.get(shuffleKey)
    val locations = reduceLocMap.get(reduceId)
    if (locations != null) {
      val res = locations.find(_.getEpoch == epoch).orNull
      if (res != null) {
        locations.remove(res)
        slotsUsed -= 1
      }
    }
    if (locations == null || locations.size() == 0) {
      reduceLocMap.remove(reduceId)
    }

    if (reduceLocMap.size() == 0) {
      partitionInfo.remove(shuffleKey)
    }
  }

  private def removePartitions(
      shuffleKey: String,
      uniqueIds: util.Collection[String],
      partitionInfo: PartitionInfo): Unit = this.synchronized {
    if (!partitionInfo.containsKey(shuffleKey)) {
      return
    }
    val reduceLocMap = partitionInfo.get(shuffleKey)
    uniqueIds.foreach { id =>
      val tokens = id.split("-", 2)
      val reduceId = tokens(0).toInt
      val epoch = tokens(1).toInt
      val locations = reduceLocMap.get(reduceId)
      if (locations != null) {
        val res = locations.find(_.getEpoch == epoch).orNull
        if (res != null) {
          locations.remove(res)
          slotsUsed -= 1
        }
      }
      if (locations == null || locations.size() == 0) {
        reduceLocMap.remove(reduceId)
      }
    }

    if (reduceLocMap.size() == 0) {
      partitionInfo.remove(shuffleKey)
    }
  }

  private def getAllIds(
      shuffleKey: String,
      partitionInfo: PartitionInfo): util.List[String] = this.synchronized {
    if (!partitionInfo.containsKey(shuffleKey)) {
      return null
    }
    new util.ArrayList(partitionInfo.get(shuffleKey).values().flatten.map(_.getUniqueId))
  }

  private def getLocation(
      shuffleKey: String,
      uniqueId: String,
      mode: PartitionLocation.Mode): PartitionLocation = {
    val tokens = uniqueId.split("-", 2)
    val reduceId = tokens(0).toInt
    val epoch = tokens(1).toInt
    val partitionInfo =
      if (mode == PartitionLocation.Mode.Master) {
        masterPartitionLocations
      } else {
        slavePartitionLocations
      }

    this.synchronized {
      if (!partitionInfo.containsKey(shuffleKey)
        || !partitionInfo.get(shuffleKey).containsKey(reduceId)) {
        return null
      }
      partitionInfo.get(shuffleKey)
        .get(reduceId)
        .find(loc => loc.getEpoch == epoch).orNull
    }
  }

  def addMasterPartition(shuffleKey: String, location: PartitionLocation): Unit = {
    addPartition(shuffleKey, location, masterPartitionLocations)
  }

  def addMasterPartitions(shuffleKey: String, locations: util.List[PartitionLocation]): Unit = {
    addPartitions(shuffleKey, locations, masterPartitionLocations)
  }

  def addSlavePartition(shuffleKey: String, location: PartitionLocation): Unit = {
    addPartition(shuffleKey, location, slavePartitionLocations)
  }

  def addSlavePartitions(shuffleKey: String, locations: util.List[PartitionLocation]): Unit = {
    addPartitions(shuffleKey, locations, slavePartitionLocations)
  }

  def removeMasterPartition(shuffleKey: String, uniqueId: String): Unit = {
    removePartition(shuffleKey, uniqueId, masterPartitionLocations)
  }

  def removeMasterPartitions(shuffleKey: String, uniqueIds: util.Collection[String]): Unit = {
    removePartitions(shuffleKey, uniqueIds, masterPartitionLocations)
  }

  def removeMasterPartitions(shuffleKey: String): Unit = {
    val uniqueIds = getAllMasterIds(shuffleKey)
    removeMasterPartitions(shuffleKey, uniqueIds)
  }

  def removeSlavePartition(shuffleKey: String, uniqueId: String): Unit = {
    removePartition(shuffleKey, uniqueId, slavePartitionLocations)
  }

  def removeSlavePartitions(shuffleKey: String, uniqueIds: util.Collection[String]): Unit = {
    removePartitions(shuffleKey, uniqueIds, slavePartitionLocations)
  }

  def removeSlavePartitions(shuffleKey: String): Unit = {
    val uniqueIds = getAllSlaveIds(shuffleKey)
    removeSlavePartitions(shuffleKey, uniqueIds)
  }

  def getAllMasterIds(shuffleKey: String): util.List[String] = {
    getAllIds(shuffleKey, masterPartitionLocations)
  }

  def getAllSlaveIds(shuffleKey: String): util.List[String] = {
    getAllIds(shuffleKey, slavePartitionLocations)
  }

  def getAllMasterLocations(shuffleKey: String): util.List[PartitionLocation] = this.synchronized {
    if (masterPartitionLocations.containsKey(shuffleKey)) {
      masterPartitionLocations.get(shuffleKey).values().flatten.toList
    } else {
      new util.ArrayList[PartitionLocation]()
    }
  }

  def getAllMasterLocationsWithMaxEpoch(
      shuffleKey: String): util.List[PartitionLocation] = this.synchronized {
    if (masterPartitionLocations.containsKey(shuffleKey)) {
      masterPartitionLocations.get(shuffleKey)
        .values()
        .map { list =>
          var loc = list(0)
          1 until list.size() foreach (ind => {
            if (list(ind).getEpoch > loc.getEpoch) {
              loc = list(ind)
            }
          })
          loc
        }.toList
    } else {
      new util.ArrayList[PartitionLocation]()
    }
  }

  def getAllSlaveLocations(shuffleKey: String): util.List[PartitionLocation] = this.synchronized {
    if (slavePartitionLocations.containsKey(shuffleKey)) {
      new util.ArrayList[PartitionLocation](
        slavePartitionLocations.get(shuffleKey).values().flatten)
    } else new util.ArrayList[PartitionLocation]()
  }

  def setMasterPeer(shuffleKey: String, loc: PartitionLocation,
    peer: PartitionLocation): Unit = this.synchronized {
    masterPartitionLocations.get(shuffleKey).get(loc.getReduceId)
      .find(l => l.getEpoch == loc.getEpoch).get
      .setPeer(peer)
  }

  def getMasterLocation(shuffleKey: String, uniqueId: String): PartitionLocation = {
    getLocation(shuffleKey, uniqueId, PartitionLocation.Mode.Master)
  }

  def getSlaveLocation(shuffleKey: String, uniqueId: String): PartitionLocation = {
    getLocation(shuffleKey, uniqueId, PartitionLocation.Mode.Slave)
  }

  def getLocationWithMaxEpoch(
      shuffleKey: String, reduceId: Int): Option[PartitionLocation] = this.synchronized {
    if (!masterPartitionLocations.containsKey(shuffleKey) ||
      !masterPartitionLocations.get(shuffleKey).containsKey(reduceId)) {
      return None
    }
    val locations = masterPartitionLocations.get(shuffleKey).get(reduceId)
    if (locations == null || locations.size() == 0) {
      return None
    }
    var currentEpoch = -1
    var currentPartition: PartitionLocation = null
    locations.foreach(loc => {
      if (loc.getEpoch > currentEpoch) {
        currentEpoch = loc.getEpoch
        currentPartition = loc
      }
    })
    Some(currentPartition)
  }

  def containsShuffleMaster(shuffleKey: String): Boolean = this.synchronized {
    masterPartitionLocations.containsKey(shuffleKey)
  }

  def containsShuffleSlave(shuffleKey: String): Boolean = this.synchronized {
    slavePartitionLocations.containsKey(shuffleKey)
  }

  def containsShuffle(shuffleKey: String): Boolean = this.synchronized {
    masterPartitionLocations.containsKey(shuffleKey) ||
      slavePartitionLocations.containsKey(shuffleKey)
  }

  def getMasterShuffleKeys(): util.Set[String] = this.synchronized {
    masterPartitionLocations.keySet()
  }

  def getSlaveShuffleKeys(): util.Set[String] = this.synchronized {
    slavePartitionLocations.keySet()
  }

  def clearAll(): Unit = this.synchronized {
    slotsUsed = 0
    masterPartitionLocations.clear()
    slavePartitionLocations.clear()
  }

  def hasSameInfoWith(other: WorkerInfo): Boolean = {
    numSlots == other.numSlots &&
      slotsUsed == other.slotsUsed &&
      hostPort == other.hostPort &&
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
      slavePartitionLocations.keySet().forall(key => {
        other.slavePartitionLocations.keySet().contains(key)
      }) &&
      slavePartitionLocations.forall(entry => {
        val shuffleKey = entry._1
        val slaves = entry._2
        val otherSlaves = other.slavePartitionLocations.get(shuffleKey)
        slaves.forall(loc => otherSlaves.contains(loc._1))
      })
  }

  override def toString(): String = {
    val sbMaster = new StringBuilder
    masterPartitionLocations.foreach(entry => {
      sbMaster.append("\t").append(entry._1).append("\t").append(entry._2.size()).append("\n")
    })
    val sbSlave = new StringBuilder
    slavePartitionLocations.foreach(entry => {
      sbSlave.append("\t").append(entry._1).append("\t").append(entry._2.size()).append("\n")
    })
    s"""
       |Address: $hostPort
       |FetchPort: $fetchPort
       |TotalSlots: $numSlots
       |SlotsUsed: $slotsUsed
       |SlotsAvailable: ${numSlots - slotsUsed}
       |MasterShuffles: ${masterPartitionLocations.size()}
       |${sbMaster.toString}
       |SlaveShuffles: ${slavePartitionLocations.size()}
       |${sbSlave.toString}
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
