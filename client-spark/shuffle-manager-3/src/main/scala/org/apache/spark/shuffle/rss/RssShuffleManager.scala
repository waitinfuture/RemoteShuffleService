/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.rss

import io.netty.util.internal.ConcurrentSet
import org.apache.spark._
import org.apache.spark.shuffle.{ShuffleReadMetricsReporter, _}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.Utils
import com.aliyun.emr.rss.client.ShuffleClient
import com.aliyun.emr.rss.client.write.LifecycleManager
import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.RssConf.compressPoolNumThreads
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.util.ThreadUtils

import java.util
import java.util.concurrent.{ConcurrentHashMap, ExecutorService}

class RssShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  private lazy val isDriver: Boolean = SparkEnv.get.executorId == SparkContext.DRIVER_IDENTIFIER

  // Read RssConf from SparkConf
  private lazy val rssConf = RssShuffleManager.fromSparkConf(conf)

  private var newAppId: Option[String] = None
  private var lifecycleManager: Option[LifecycleManager] = None
  private var rssShuffleClient: Option[ShuffleClient] = None

  val sortShuffleManagerName = classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName
  private lazy val sortShuffleManager = {
    RssShuffleManager.instantiateClass[SortShuffleManager](sortShuffleManagerName, conf, isDriver)
  }
  private val sortShuffleIds = new ConcurrentSet[Int]()

  private lazy val fallbackPolicyRunner = new RssShuffleFallbackPolicyRunner(conf)

  // (shuffleId -> sendBuffer) Only used on Executors
  private val reusedSendBuffers = new ConcurrentHashMap[Integer, util.LinkedList[Array[Array[Byte]]]]

  private lazy val compressPool: ExecutorService = {
    ThreadUtils.newDaemonCachedThreadPool("Compress-Pool", compressPoolNumThreads(rssConf), 60)
  };

  private def initializeLifecycleManager(appId: String): Unit = {
    // Only create LifecycleManager singleton in Driver. When register shuffle multiple times, we
    // need to ensure that LifecycleManager will only be created once. Parallelism needs to be
    // considered in this place, because if there is one RDD that depends on multiple RDDs
    // at the same time, it may bring parallel `register shuffle`, such as Join in Sql.
    if (isDriver && lifecycleManager.isEmpty) {
      lifecycleManager.synchronized {
        if (lifecycleManager.isEmpty) {
          val metaSystem = new LifecycleManager(appId, rssConf)
          lifecycleManager = Some(metaSystem)
          rssShuffleClient = Some(ShuffleClient.get(metaSystem.self, rssConf))
        }
      }
    }
  }

  override def registerShuffle[K, V, C](
      shuffleId: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // Note: generate newAppId at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    // This method may be called many times.
    newAppId = Some(RssShuffleManager.genNewAppId(dependency.rdd.context))
    newAppId.foreach(initializeLifecycleManager)

    if (fallbackPolicyRunner.applyAllFallbackPolicy(lifecycleManager.get,
      dependency.partitioner.numPartitions)) {
      logWarning("Fallback to SortShuffleManager!")
      sortShuffleIds.add(shuffleId)
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    } else {
      new RssShuffleHandle[K, V, C](
        newAppId.get,
        // If not driver, return dummy rss meta service host and port.
        lifecycleManager.map(_.getRssMetaServiceHost).getOrElse(""),
        lifecycleManager.map(_.getRssMetaServicePort).getOrElse(0),
        shuffleId,
        dependency.rdd.getNumPartitions,
        dependency)
    }
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (sortShuffleIds.contains(shuffleId)) {
      sortShuffleManager.unregisterShuffle(shuffleId)
    } else {
      newAppId match {
        case Some(id) =>
          logWarning("in registerShuffle, remove send buffer")
          reusedSendBuffers.remove(shuffleId)
          rssShuffleClient.exists(_.unregisterShuffle(id, shuffleId, isDriver))
        case None => true
      }
    }
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    sortShuffleManager.shuffleBlockResolver
  }

  override def stop(): Unit = {
    rssShuffleClient.foreach(_.shutDown())
    lifecycleManager.foreach(_.stop())
    if (sortShuffleManager != null) {
      sortShuffleManager.stop()
    }
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case h: RssShuffleHandle[K@unchecked, V@unchecked, _] =>
        val client = ShuffleClient.get(h.essMetaServiceHost, h.essMetaServicePort, rssConf)
        if (RssConf.shuffleWriterMode(rssConf) == "sort") {
          new SortBasedShuffleWriter(h.dependency, h.newAppId, h.numMappers,
            context, rssConf, client, metrics)
        } else if (RssConf.shuffleWriterMode(rssConf) == "hash") {
          logWarning(s"inside get Writer, empty: ${reusedSendBuffers.contains(handle.shuffleId)}")
          reusedSendBuffers.putIfAbsent(handle.shuffleId, new util.LinkedList[Array[Array[Byte]]]())
          new HashBasedShuffleWriter(h, context, rssConf, client, metrics, reusedSendBuffers.get(handle.shuffleId), compressPool)
        } else {
          throw new UnsupportedOperationException(
            s"Unrecognized shuffle write mode! ${RssConf.shuffleWriterMode(rssConf)}")
        }
      case _ => sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Interface for Spark3.1 and higher
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case _: RssShuffleHandle[K@unchecked, C@unchecked, _] =>
        new RssShuffleReader(
          handle.asInstanceOf[RssShuffleHandle[K, _, C]],
          startPartition,
          endPartition,
          startMapIndex,
          endMapIndex,
          context,
          rssConf,
          metrics)
      case _ =>
        RssShuffleManager.invokeGetReaderMethod(
          sortShuffleManagerName,
          "getReader",
          sortShuffleManager,
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition,
          context,
          metrics)
    }
  }

  /**
   * Interface for Spark3.0 and higher
   */
  def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    handle match {
      case _: RssShuffleHandle[K@unchecked, C@unchecked, _] =>
        new RssShuffleReader(
          handle.asInstanceOf[RssShuffleHandle[K, _, C]],
          startPartition,
          endPartition,
          context = context,
          conf = rssConf,
          metrics = metrics)
      case _ =>
        RssShuffleManager.invokeGetReaderMethod(
          sortShuffleManagerName,
          "getReader",
          sortShuffleManager,
          handle,
          -1,
          -1,
          startPartition,
          endPartition,
          context,
          metrics)
    }
  }

  def getReaderForRange[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    new RssShuffleReader(
      handle.asInstanceOf[RssShuffleHandle[K, _, C]],
      startPartition,
      endPartition,
      startMapIndex,
      endMapIndex,
      context = context,
      conf = rssConf,
      metrics)
  }
}

object RssShuffleManager {

  /**
   * make rss conf from spark conf
   *
   * @param conf
   * @return
   */
  def fromSparkConf(conf: SparkConf): RssConf = {
    val tmpRssConf = new RssConf()
    for ((key, value) <- conf.getAll if key.startsWith("spark.rss.")) {
      tmpRssConf.set(key.substring("spark.".length), value)
    }
    tmpRssConf
  }

  def genNewAppId(context: SparkContext): String = {
    context.applicationAttemptId match {
      case Some(id) => s"${context.applicationId}_$id"
      case None => s"${context.applicationId}"
    }
  }

  // Create an instance of the class with the given name, possibly initializing it with our conf
  // Copied from SparkEnv
  def instantiateClass[T](className: String, conf: SparkConf, isDriver: Boolean): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }

  // Invoke and return getReader method of SortShuffleManager
  def invokeGetReaderMethod[K, C](
      className: String,
      methodName: String,
      sortShuffleManager: SortShuffleManager,
      handle: ShuffleHandle,
      startMapIndex: Int = 0,
      endMapIndex: Int = Int.MaxValue,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val cls = Utils.classForName(className)
    try {
      val method = cls.getMethod(methodName, classOf[ShuffleHandle], Integer.TYPE, Integer.TYPE,
        Integer.TYPE, Integer.TYPE, classOf[TaskContext], classOf[ShuffleReadMetricsReporter])
      method.invoke(
        sortShuffleManager,
        handle,
        Integer.valueOf(startMapIndex),
        Integer.valueOf(endMapIndex),
        Integer.valueOf(startPartition),
        Integer.valueOf(endPartition),
        context,
        metrics).asInstanceOf[ShuffleReader[K, C]]
    } catch {
      case _: NoSuchMethodException =>
        try {
          val method = cls.getMethod(methodName, classOf[ShuffleHandle], Integer.TYPE, Integer.TYPE,
            classOf[TaskContext], classOf[ShuffleReadMetricsReporter])
          method.invoke(
            sortShuffleManager,
            handle,
            Integer.valueOf(startPartition),
            Integer.valueOf(endPartition),
            context,
            metrics).asInstanceOf[ShuffleReader[K, C]]
        } catch {
          case e: NoSuchMethodException =>
            throw new Exception("Get getReader method failed.", e)
        }
    }
  }
}

class RssShuffleHandle[K, V, C](
    val newAppId: String,
    val essMetaServiceHost: String,
    val essMetaServicePort: Int,
    shuffleId: Int,
    val numMappers: Int,
    dependency: ShuffleDependency[K, V, C])
  extends BaseShuffleHandle(shuffleId, dependency) {
}
