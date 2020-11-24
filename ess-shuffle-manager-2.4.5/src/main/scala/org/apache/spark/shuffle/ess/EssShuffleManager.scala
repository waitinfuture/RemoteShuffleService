package org.apache.spark.shuffle.ess

import com.aliyun.emr.ess.client.ShuffleClient
import com.aliyun.emr.ess.common.EssConf

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._

class EssShuffleManager(conf: SparkConf)
  extends ShuffleManager with Logging {

  // Read EssConf from SparkConf
  private lazy val essConf = EssShuffleManager.fromSparkConf(conf)
  private lazy val essShuffleClient = ShuffleClient.get(essConf)
  private var newAppId: Option[String] = None

  override def registerShuffle[K, V, C](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {

    // Note: generate newAppId at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    newAppId = Some(EssShuffleManager.genNewAppId(dependency.rdd.context))
    essShuffleClient.startHeartbeat(newAppId.get)

    new EssShuffleHandle[K, V](
      newAppId.get,
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
        new EssShuffleWriter(h, mapId, context, conf, h.numMaps,
          h.dependency.partitioner.numPartitions)
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

  // remove override for compatibility
  def getReader[K, C](
    handle: ShuffleHandle,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    startMapId: Int,
    endMapId: Int): ShuffleReader[K, C] = {
    throw new UnsupportedOperationException("Do not support Ess Shuffle and AE at the same time.")
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    newAppId match {
      case Some(id) => essShuffleClient.unregisterShuffle(id, shuffleId,
        SparkEnv.get.executorId == SparkContext.DRIVER_IDENTIFIER)
      case None => true
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
   *
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

  def genNewAppId(context: SparkContext): String = {
    context.applicationAttemptId match {
      case Some(id) => s"${context.applicationId}_$id"
      case None => s"${context.applicationId}"
    }
  }
}

class EssShuffleHandle[K, V](
  val newAppId: String,
  shuffleId: Int,
  numMaps: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}
