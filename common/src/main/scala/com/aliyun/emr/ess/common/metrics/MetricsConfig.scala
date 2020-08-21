package com.aliyun.emr.ess.common.metrics

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.util.Utils

import scala.collection.mutable
import scala.util.matching.Regex
import scala.collection.JavaConverters._

private class MetricsConfig(conf: EssConf) extends Logging {

  private val DEFAULT_PREFIX = "*"
  private val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  private val DEFAULT_METRICS_CONF_FILENAME = "metrics.properties"

  private[metrics] val properties = new Properties()
  private[metrics] var perInstanceSubProperties: mutable.HashMap[String, Properties] = null

  private def setDefaultProperties(prop: Properties): Unit = {
    // TODO: add default properties
  }

  /**
    * Load properties from various places, based on precedence
    * If the same property is set again latter on in the method, it overwrites the previous value
    */
  def initialize() {
    // Add default properties in case there's no properties file
    setDefaultProperties(properties)

    loadPropertiesFromFile(conf.getOption("ess.metrics.conf"))

    // Also look for the properties in provided Ess configuration
    val prefix = "ess.metrics.conf."
    conf.getAll.foreach {
      case (k, v) if k.startsWith(prefix) =>
        properties.setProperty(k.substring(prefix.length()), v)
      case _ =>
    }

    perInstanceSubProperties = subProperties(properties, INSTANCE_REGEX)
    if (perInstanceSubProperties.contains(DEFAULT_PREFIX)) {
      val defaultSubProperties = perInstanceSubProperties(DEFAULT_PREFIX).asScala
      for ((instance, prop) <- perInstanceSubProperties if (instance != DEFAULT_PREFIX);
           (k, v) <- defaultSubProperties if (prop.get(k) == null)) {
        prop.put(k, v)
      }
    }
  }

  def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] = {
    val subProperties = new mutable.HashMap[String, Properties]
    prop.asScala.foreach { kv =>
      if (regex.findPrefixOf(kv._1.toString).isDefined) {
        val regex(prefix, suffix) = kv._1.toString
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2.toString)
      }
    }
    subProperties
  }

  def getInstance(inst: String): Properties = {
    perInstanceSubProperties.get(inst) match {
      case Some(s) => s
      case None => perInstanceSubProperties.getOrElse(DEFAULT_PREFIX, new Properties)
    }
  }

  /**
    * Loads configuration from a config file. If no config file is provided, try to get file
    * in class path.
    */
  private[this] def loadPropertiesFromFile(path: Option[String]): Unit = {
    var is: InputStream = null
    try {
      is = path match {
        case Some(f) => new FileInputStream(f)
        case None => Utils.getClassLoader.getResourceAsStream(DEFAULT_METRICS_CONF_FILENAME)
      }

      if (is != null) {
        properties.load(is)
      }
    } catch {
      case e: Exception =>
        val file = path.getOrElse(DEFAULT_METRICS_CONF_FILENAME)
        logError(s"Error loading configuration file $file", e)
    } finally {
      if (is != null) {
        is.close()
      }
    }
  }

}
