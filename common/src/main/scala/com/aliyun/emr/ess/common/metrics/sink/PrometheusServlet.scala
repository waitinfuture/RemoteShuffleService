package com.aliyun.emr.ess.common.metrics.sink

import java.util.Properties

import scala.collection.mutable

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.metrics.source.Source
import com.codahale.metrics.MetricRegistry
import io.netty.channel.ChannelHandler.Sharable

class PrometheusServlet(val property: Properties,
  val registry: MetricRegistry,
  val sources: mutable.ArrayBuffer[Source]) extends Sink with Logging{

  val SERVLET_PATH = "/metrics/prometheus"

  def getHandler(conf: EssConf): PrometheusHttpRequestHandler = {
      new PrometheusHttpRequestHandler(SERVLET_PATH, this)
  }

  def getMetricsSnapshot(): String = {
    val sb = new StringBuilder()
    sources.foreach(source => sb.append(source.getMetrics()))
    sb.toString()
  }

  override def start(): Unit = { }

  override def stop(): Unit = { }

  override def report(): Unit = { }
}

@Sharable
class PrometheusHttpRequestHandler(path: String, prometheusServlet: PrometheusServlet) extends Logging {

  def handleRequest(uri: String): String = {
    uri match {
      case `path` =>
        prometheusServlet.getMetricsSnapshot()
      case _ => s"Unknown uri ${uri}!"
    }
  }
}
