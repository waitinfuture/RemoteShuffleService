package org.apache.spark.shuffle.ess

import org.apache.spark.{SparkContext, SparkEnv, _}
import org.apache.spark.shuffle._
import org.apache.spark.sql.internal.SQLConf

import com.aliyun.emr.ess.client.ShuffleClient
import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging

class EssShuffleManager(conf: SparkConf)
  extends ShuffleManager with Logging {

  // Read EssConf from SparkConf
  private lazy val essConf = EssShuffleManager.fromSparkConf(conf)
  private lazy val essShuffleClient = ShuffleClient.get(essConf)
  private var newAppId: Option[String] = None

  override def registerShuffle[K, V, C](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {

    if (conf.get(SQLConf.LOCAL_SHUFFLE_READER_ENABLED) || conf.get(SQLConf.SKEW_JOIN_ENABLED)) {
      // todo: support local shuffle reader and skew join.
      // fast fail.
      throw new UnsupportedOperationException(s"Currently RSS doesn't support LocalShuffleReader" +
        s" and skew join. Please set the value of ${SQLConf.LOCAL_SHUFFLE_READER_ENABLED.key} and" +
        s" ${SQLConf.SKEW_JOIN_ENABLED.key} to false in order to use RSS, or remove `--conf " +
        "spark.shuffle.manager=org.apache.spark.shuffle.ess.EssShuffleManager` to disable rss.")
    }

    // Note: generate newAppId at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    newAppId = Some(EssShuffleManager.genNewAppId(dependency.rdd.context))
    essShuffleClient.startHeartbeat(newAppId.get)

    new EssShuffleHandle[K, V](
      newAppId.get,
      shuffleId,
      dependency.rdd.getNumPartitions,
      dependency.asInstanceOf[ShuffleDependency[K, V, V]])
  }

  override def getWriter[K, V](
    handle: ShuffleHandle,
    mapId: Long,
    context: TaskContext,
    metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case h: EssShuffleHandle[K@unchecked, V@unchecked] =>
        new EssShuffleWriter(h, mapId, context, conf, h.numMappers,
          h.dependency.partitioner.numPartitions)
    }
  }

  override def getReader[K, C](
    handle: ShuffleHandle,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
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

  override def getReaderForRange[K, C](
    handle: ShuffleHandle,
    startMapIndex: Int,
    endMapIndex: Int,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    throw new UnsupportedOperationException("Do not support Ess Shuffle and AE at the same time.")
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
  val numMappers: Int,
  dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, dependency) {
}
