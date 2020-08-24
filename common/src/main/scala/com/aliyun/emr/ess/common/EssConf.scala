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

package com.aliyun.emr.ess.common

import java.util.concurrent.ConcurrentHashMap
import java.util.{Map => JMap}

import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.internal.config._
import com.aliyun.emr.ess.common.util.Utils

import scala.collection.JavaConverters._

class EssConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  import EssConf._

  /** Create a EssConf that loads defaults from system properties and the classpath */
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new EssConfigProvider(settings))
    _reader.bindEnv(new ConfigProvider {
      override def get(key: String): Option[String] = Option(getenv(key))
    })
    _reader
  }

  if (loadDefaults) {
    loadFromSystemProperties(false)
  }

  private[ess] def loadFromSystemProperties(silent: Boolean): EssConf = {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("ess.")) {
      set(key, value, silent)
    }
    this
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): EssConf = {
    set(key, value, false)
  }

  private[ess] def set(key: String, value: String, silent: Boolean): EssConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    if (!silent) {
      logDeprecationWarning(key)
    }
    settings.put(key, value)
    this
  }

  private[ess] def set[T](entry: ConfigEntry[T], value: T): EssConf = {
    set(entry.key, entry.stringConverter(value))
    this
  }

  private[ess] def set[T](entry: OptionalConfigEntry[T], value: T): EssConf = {
    set(entry.key, entry.rawStringConverter(value))
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): EssConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): EssConf = {
    if (settings.putIfAbsent(key, value) == null) {
      logDeprecationWarning(key)
    }
    this
  }

  private[ess] def setIfMissing[T](entry: ConfigEntry[T], value: T): EssConf = {
    if (settings.putIfAbsent(entry.key, entry.stringConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  private[ess] def setIfMissing[T](entry: OptionalConfigEntry[T], value: T): EssConf = {
    if (settings.putIfAbsent(entry.key, entry.rawStringConverter(value)) == null) {
      logDeprecationWarning(entry.key)
    }
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): EssConf = {
    settings.remove(key)
    this
  }

  private[ess] def remove(entry: ConfigEntry[_]): EssConf = {
    remove(entry.key)
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /**
    * Retrieves the value of a pre-defined configuration entry.
    *
    * - This is an internal Spark API.
    * - The return type if defined by the configuration entry.
    * - This will throw an exception is the config is not optional and the value is not set.
    */
  private[ess] def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(reader)
  }

  /**
    * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
    * suffix is provided then seconds are assumed.
    * @throws java.util.NoSuchElementException If the time parameter is not set
    * @throws NumberFormatException If the value cannot be interpreted as seconds
    */
  def getTimeAsSeconds(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key))
  }

  /**
    * Get a time parameter as seconds, falling back to a default if not set. If no
    * suffix is provided then seconds are assumed.
    * @throws NumberFormatException If the value cannot be interpreted as seconds
    */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }

  /**
    * Get a time parameter as milliseconds; throws a NoSuchElementException if it's not set. If no
    * suffix is provided then milliseconds are assumed.
    * @throws java.util.NoSuchElementException If the time parameter is not set
    * @throws NumberFormatException If the value cannot be interpreted as milliseconds
    */
  def getTimeAsMs(key: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key))
  }

  /**
    * Get a time parameter as milliseconds, falling back to a default if not set. If no
    * suffix is provided then milliseconds are assumed.
    * @throws NumberFormatException If the value cannot be interpreted as milliseconds
    */
  def getTimeAsMs(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.timeStringAsMs(get(key, defaultValue))
  }

  /**
    * Get a size parameter as bytes; throws a NoSuchElementException if it's not set. If no
    * suffix is provided then bytes are assumed.
    * @throws java.util.NoSuchElementException If the size parameter is not set
    * @throws NumberFormatException If the value cannot be interpreted as bytes
    */
  def getSizeAsBytes(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key))
  }

  /**
    * Get a size parameter as bytes, falling back to a default if not set. If no
    * suffix is provided then bytes are assumed.
    * @throws NumberFormatException If the value cannot be interpreted as bytes
    */
  def getSizeAsBytes(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue))
  }

  /**
    * Get a size parameter as bytes, falling back to a default if not set.
    * @throws NumberFormatException If the value cannot be interpreted as bytes
    */
  def getSizeAsBytes(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    Utils.byteStringAsBytes(get(key, defaultValue + "B"))
  }

  /**
    * Get a size parameter as Kibibytes; throws a NoSuchElementException if it's not set. If no
    * suffix is provided then Kibibytes are assumed.
    * @throws java.util.NoSuchElementException If the size parameter is not set
    * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
    */
  def getSizeAsKb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key))
  }

  /**
    * Get a size parameter as Kibibytes, falling back to a default if not set. If no
    * suffix is provided then Kibibytes are assumed.
    * @throws NumberFormatException If the value cannot be interpreted as Kibibytes
    */
  def getSizeAsKb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsKb(get(key, defaultValue))
  }

  /**
    * Get a size parameter as Mebibytes; throws a NoSuchElementException if it's not set. If no
    * suffix is provided then Mebibytes are assumed.
    * @throws java.util.NoSuchElementException If the size parameter is not set
    * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
    */
  def getSizeAsMb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key))
  }

  /**
    * Get a size parameter as Mebibytes, falling back to a default if not set. If no
    * suffix is provided then Mebibytes are assumed.
    * @throws NumberFormatException If the value cannot be interpreted as Mebibytes
    */
  def getSizeAsMb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsMb(get(key, defaultValue))
  }

  /**
    * Get a size parameter as Gibibytes; throws a NoSuchElementException if it's not set. If no
    * suffix is provided then Gibibytes are assumed.
    * @throws java.util.NoSuchElementException If the size parameter is not set
    * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
    */
  def getSizeAsGb(key: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key))
  }

  /**
    * Get a size parameter as Gibibytes, falling back to a default if not set. If no
    * suffix is provided then Gibibytes are assumed.
    * @throws NumberFormatException If the value cannot be interpreted as Gibibytes
    */
  def getSizeAsGb(key: String, defaultValue: String): Long = catchIllegalValue(key) {
    Utils.byteStringAsGb(get(key, defaultValue))
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)).orElse(getDeprecatedConfig(key, settings))
  }

  /** Get an optional value, applying variable substitution. */
  private[ess] def getWithSubstitution(key: String): Option[String] = {
    getOption(key).map(reader.substitute(_))
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /**
    * Get all parameters that start with `prefix`
    */
  def getAllWithPrefix(prefix: String): Array[(String, String)] = {
    getAll.filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }
  }


  /**
    * Get a parameter as an integer, falling back to a default if not set
    * @throws NumberFormatException If the value cannot be interpreted as an integer
    */
  def getInt(key: String, defaultValue: Int): Int = catchIllegalValue(key) {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /**
    * Get a parameter as a long, falling back to a default if not set
    * @throws NumberFormatException If the value cannot be interpreted as a long
    */
  def getLong(key: String, defaultValue: Long): Long = catchIllegalValue(key) {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /**
    * Get a parameter as a double, falling back to a default if not ste
    * @throws NumberFormatException If the value cannot be interpreted as a double
    */
  def getDouble(key: String, defaultValue: Double): Double = catchIllegalValue(key) {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /**
    * Get a parameter as a boolean, falling back to a default if not set
    * @throws IllegalArgumentException If the value cannot be interpreted as a boolean
    */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = catchIllegalValue(key) {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = {
    settings.containsKey(key) ||
      configsWithAlternatives.get(key).toSeq.flatten.exists { alt => contains(alt.key) }
  }

  private[ess] def contains(entry: ConfigEntry[_]): Boolean = contains(entry.key)

  /** Copy this object */
  override def clone: EssConf = {
    val cloned = new EssConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey(), e.getValue(), true)
    }
    cloned
  }

  /**
    * By using this instead of System.getenv(), environment variables can be mocked
    * in unit tests.
    */
  private[ess] def getenv(name: String): String = System.getenv(name)

  /**
    * Wrapper method for get() methods which require some specific value format. This catches
    * any [[NumberFormatException]] or [[IllegalArgumentException]] and re-raises it with the
    * incorrectly configured key in the exception message.
    */
  private def catchIllegalValue[T](key: String)(getValue: => T): T = {
    try {
      getValue
    } catch {
      case e: NumberFormatException =>
        // NumberFormatException doesn't have a constructor that takes a cause for some reason.
        throw new NumberFormatException(s"Illegal value for config key $key: ${e.getMessage}")
          .initCause(e)
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Illegal value for config key $key: ${e.getMessage}", e)
    }
  }
}

object EssConf extends Logging {

  /**
    * Maps deprecated config keys to information about the deprecation.
    *
    * The extra information is logged as a warning when the config is present in the user's
    * configuration.
    */
  private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
    val configs = Seq(
      DeprecatedConfig("none", "1.0",
          "None")
    )

    Map(configs.map { cfg => (cfg.key -> cfg) } : _*)
  }

  /**
    * Maps a current config key to alternate keys that were used in previous version of Spark.
    *
    * The alternates are used in the order defined in this map. If deprecated configs are
    * present in the user's configuration, a warning is logged.
    *
    * TODO: consolidate it with `ConfigBuilder.withAlternative`.
    */
  private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
    "none" -> Seq(
      AlternateConfig("none", "1.0"))
  )

  /**
    * A view of `configsWithAlternatives` that makes it more efficient to look up deprecated
    * config keys.
    *
    * Maps the deprecated config name to a 2-tuple (new config name, alternate config info).
    */
  private val allAlternatives: Map[String, (String, AlternateConfig)] = {
    configsWithAlternatives.keys.flatMap { key =>
      configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
    }.toMap
  }

  /**
    * Looks for available deprecated keys for the given config option, and return the first
    * value available.
    */
  def getDeprecatedConfig(key: String, conf: JMap[String, String]): Option[String] = {
    configsWithAlternatives.get(key).flatMap { alts =>
      alts.collectFirst { case alt if conf.containsKey(alt.key) =>
        val value = conf.get(alt.key)
        if (alt.translation != null) alt.translation(value) else value
      }
    }
  }

  /**
    * Logs a warning message if the given config key is deprecated.
    */
  def logDeprecationWarning(key: String): Unit = {
    deprecatedConfigs.get(key).foreach { cfg =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of ESS ${cfg.version} and " +
          s"may be removed in the future. ${cfg.deprecationMessage}")
      return
    }

    allAlternatives.get(key).foreach { case (newKey, cfg) =>
      logWarning(
        s"The configuration key '$key' has been deprecated as of ESS ${cfg.version} and " +
          s"may be removed in the future. Please use the new key '$newKey' instead.")
      return
    }
  }

  /**
    * Holds information about keys that have been deprecated and do not have a replacement.
    *
    * @param key The deprecated key.
    * @param version Version of Spark where key was deprecated.
    * @param deprecationMessage Message to include in the deprecation warning.
    */
  private case class DeprecatedConfig(
      key: String,
      version: String,
      deprecationMessage: String)

  /**
    * Information about an alternate configuration key that has been deprecated.
    *
    * @param key The deprecated config key.
    * @param version The Spark version in which the key was deprecated.
    * @param translation A translation function for converting old config values into new ones.
    */
  private case class AlternateConfig(
      key: String,
      version: String,
      translation: String => String = null)

  // Conf getters

  def essPushDataBufferSize(conf: EssConf): Long = {
    conf.getSizeAsBytes("ess.push.data.buffer.size", "64k")
  }

  def essPushDataQueueCapacity(conf: EssConf): Int = {
    conf.getInt("ess.push.data.queue.capacity", 512)
  }

  def essPushDataMaxReqsInFlight(conf: EssConf): Int = {
    conf.getInt("ess.push.data.maxReqsInFlight", 1024)
  }

  def essFetchChunkTimeoutMs(conf: EssConf): Long = {
    conf.getTimeAsMs("ess.fetch.chunk.timeout", "120s")
  }

  def essFetchChunkMaxReqsInFlight(conf: EssConf): Int = {
    conf.getInt("ess.fetch.chunk.maxReqsInFlight", 3)
  }

  def essReplicate(conf: EssConf): Boolean = {
    conf.getBoolean("ess.push.data.replicate", true)
  }

  def essWorkerTimeoutMs(conf: EssConf): Long = {
    conf.getTimeAsMs("ess.worker.timeout", "120s")
  }

  def essApplicationTimeoutMs(conf: EssConf): Long = {
    conf.getTimeAsMs("ess.application.timeout", "120s")
  }

  def essRemoveShuffleDelayMs(conf: EssConf): Long = {
    conf.getTimeAsMs("ess.remove.shuffle.delay", "60s")
  }

  def essMasterHost(conf: EssConf): String = {
    conf.get("ess.master.host", Utils.localHostName())
  }

  def essMasterPort(conf: EssConf): Int = {
    conf.getInt("ess.master.port", 9099)
  }

  def essWorkerFlushBufferSize(conf: EssConf): Long = {
    conf.getSizeAsBytes("ess.worker.flush.buffer.size", "256k")
  }

  def essWorkerFlushQueueCapacity(conf: EssConf): Int = {
    conf.getInt("ess.worker.flush.queue.capacity", 512)
  }

  def essWorkerFetchChunkSize(conf: EssConf): Long = {
    conf.getSizeAsBytes("ess.worker.fetch.chunk.size", "8m")
  }

  def essWorkerNumSlots(conf: EssConf, numDisks: Int): Int = {
    val userNumSlots = conf.getInt("ess.worker.numSlots", -1)
    if (userNumSlots > 0) {
      userNumSlots
    } else {
      essWorkerFlushQueueCapacity(conf: EssConf) * numDisks / 2
    }
  }

  def essRpcMaxParallelism(conf: EssConf): Int = {
    conf.getInt("ess.rpc.max.parallelism", 1024)
  }

  def essRegisterShuffleMaxRetry(conf: EssConf): Int = {
    conf.getInt("ess.register.shuffle.max.retry", 3)
  }

  def essFlushTimeout(conf: EssConf): Long = {
    conf.getTimeAsSeconds("ess.flush.timeout", "240s")
  }

  def essFlushTimeoutMs(conf: EssConf): Long = {
    conf.getTimeAsMs("ess.flush.timeout", "240s")
  }

  def essWorkerBaseDirs(conf: EssConf): Array[String] = {
    val baseDirs = conf.get("ess.worker.base.dirs", "")
    if (baseDirs.nonEmpty) {
      baseDirs.split(",")
    } else {
      val prefix = EssConf.essWorkerBaseDirPrefix(conf)
      val number = EssConf.essWorkerBaseDirNumber(conf)
      (1 to number).map(i => s"$prefix$i").toArray
    }
  }

  def essWorkerBaseDirPrefix(conf: EssConf): String = {
    conf.get("ess.worker.base.dir.prefix", "/mnt/disk")
  }

  def essWorkerBaseDirNumber(conf: EssConf): Int = {
    conf.getInt("ess.worker.base.dir.number", 16)
  }

  def essStageEndTimeout(conf: EssConf): Long = {
    conf.getTimeAsMs("ess.stage.end.timeout", "120s")
  }

  def essLimitInFlightTimeoutMs(conf: EssConf): Long = {
    conf.getTimeAsMs("ess.limit.in.flight.timeout", "60s")
  }

  def essLimitInFlightSleepDeltaMs(conf: EssConf): Long = {
    conf.getTimeAsMs("ess.limit.in.flight.sleep.delta", "50ms")
  }

  def essPushServerPort(conf: EssConf): Int = {
    conf.getInt("ess.pushserver.port", 0)
  }

  def essFetchServerPort(conf: EssConf): Int = {
    conf.getInt("ess.fetchserver.port", 0)
  }

  def essRegisterWorkerTimeoutMs(conf: EssConf): Long = {
    conf.getTimeAsMs("ess.register.worker.timeout", "180s")
  }

  def essMasterPortMaxRetry(conf: EssConf): Int = {
    conf.getInt("ess.master.port.maxretry", 1)
  }

  def essPushDataRetryThreadNum(conf: EssConf): Int = {
    conf.getInt("ess.pushdata.retry.thread.num", 8)
  }

  def essMetricsSystemEnable(conf: EssConf): Boolean = {
    conf.getBoolean("ess.metrics.system.enable", defaultValue = true)
  }

  def essMetricsTimerSlidingSize(conf: EssConf): Int = {
    conf.getInt("ess.metrics.system.timer.sliding.size", 4000)
  }

  // 0 to 1.0
  def essMetricsSampleRate(conf: EssConf): Double = {
    conf.getDouble("ess.metrics.system.sample.rate", 1)
  }

  def essMetricsSlidingWindowSize(conf: EssConf): Int = {
    conf.getInt("ess.metrics.system.sliding.window.size", 4096)
  }

  def essInnerMetricsSize(conf: EssConf): Int = {
    conf.getInt("ess.inner.metrics.size", 4096)
  }

  def essMasterPrometheusMetricPort(conf: EssConf): Int = {
    conf.getInt("ess.master.prometheus.metric.port", 9098)
  }

  def essWorkerPrometheusMetricPort(conf: EssConf): Int = {
    conf.getInt("ess.worker.prometheus.metric.port", 9096)
  }
}
