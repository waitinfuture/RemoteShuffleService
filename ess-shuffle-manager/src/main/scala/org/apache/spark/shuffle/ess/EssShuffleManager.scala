package org.apache.spark.shuffle.ess

import com.aliyun.emr.ess.client.ShuffleClient
import com.aliyun.emr.ess.client.impl.ShuffleClientImpl
import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.protocol.PartitionLocation
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._

class EssShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  // Read EssConf from SparkConf
  private lazy val essConf = EssShuffleManager.fromSparkConf(conf)
  private lazy val essShuffleClient = ShuffleClient.get(essConf)

  override def registerShuffle[K, V, C](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    val registered = essShuffleClient.registerShuffle(conf.getAppId,
      shuffleId,
      numMaps,
      dependency.partitioner.numPartitions)

    if (!registered) {
      throw new Exception("register shuffle failed.")
    }

    new EssShuffleHandle[K, V](
      essShuffleClient.fetchShuffleInfo(conf.getAppId, shuffleId),
      shuffleId,
      numMaps,
      dependency.asInstanceOf[ShuffleDependency[K, V, V]])
  }

  override def getWriter[K, V](
    handle: ShuffleHandle,
    mapId: Int,
    context: TaskContext): ShuffleWriter[K, V] = {
    handle match {
      case h: BaseShuffleHandle[K@unchecked, V@unchecked, _] =>
        new EssShuffleWriter(
          context.taskMemoryManager(), h, mapId, context, conf)
    }
  }

  override def getReader[K, C](
    handle: ShuffleHandle,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext): ShuffleReader[K, C] = {
    new EssShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
      startPartition, endPartition, context, conf)
  }

  override def getReader[K, C](
    handle: ShuffleHandle,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    startMapId: Int,
    endMapId: Int): ShuffleReader[K, C] = {
    throw new UnsupportedOperationException("Do not support Ess Shuffle and AE at the same time.")
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (SparkEnv.get.executorId == SparkContext.DRIVER_IDENTIFIER) {
      essShuffleClient.unregisterShuffle(conf.getAppId, shuffleId)
    } else {
      true
    }
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = null

  override def stop(): Unit = {
    essShuffleClient.shutDown()
  }
}

object EssShuffleManager {

  /**
    * make ess conf from spark conf
    * @param conf
    * @return
    */
  def fromSparkConf(conf: SparkConf): EssConf = {
    val tmpEssConf = new EssConf()
    for ((key, value) <- conf.getAll if key.startsWith("spark.ess.")) {
      tmpEssConf.set(key.substring("spark.".length), value)
    }
    tmpEssConf
  }
}

class EssShuffleHandle[K, V](val initPartitionLocations: java.util.List[PartitionLocation],
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}
