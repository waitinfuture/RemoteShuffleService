package com.aliyun.emr.ess.common.metrics.sink

import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.aliyun.emr.ess.common.metrics.MetricsSystem
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter, GraphiteUDP}

private class GraphiteSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  val GRAPHITE_DEFAULT_PERIOD = 10
  val GRAPHITE_DEFAULT_UNIT = "SECONDS"
  val GRAPHITE_DEFAULT_PREFIX = ""

  val GRAPHITE_KEY_HOST = "host"
  val GRAPHITE_KEY_PORT = "port"
  val GRAPHITE_KEY_PERIOD = "period"
  val GRAPHITE_KEY_UNIT = "unit"
  val GRAPHITE_KEY_PREFIX = "prefix"
  val GRAPHITE_KEY_PROTOCOL = "protocol"

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))

  if (!propertyToOption(GRAPHITE_KEY_HOST).isDefined) {
    throw new Exception("Graphite sink requires 'host' property.")
  }

  if (!propertyToOption(GRAPHITE_KEY_PORT).isDefined) {
    throw new Exception("Graphite sink requires 'port' property.")
  }

  val host = propertyToOption(GRAPHITE_KEY_HOST).get
  val port = propertyToOption(GRAPHITE_KEY_PORT).get.toInt

  val pollPeriod = propertyToOption(GRAPHITE_KEY_PERIOD) match {
    case Some(s) => s.toInt
    case None => GRAPHITE_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = propertyToOption(GRAPHITE_KEY_UNIT) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase(Locale.ROOT))
    case None => TimeUnit.valueOf(GRAPHITE_DEFAULT_UNIT)
  }

  val prefix = propertyToOption(GRAPHITE_KEY_PREFIX).getOrElse(GRAPHITE_DEFAULT_PREFIX)

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val graphite = propertyToOption(GRAPHITE_KEY_PROTOCOL).map(_.toLowerCase(Locale.ROOT)) match {
    case Some("udp") => new GraphiteUDP(host, port)
    case Some("tcp") | None => new Graphite(host, port)
    case Some(p) => throw new Exception(s"Invalid Graphite protocol: $p")
  }

  val reporter: GraphiteReporter = GraphiteReporter.forRegistry(registry)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .convertRatesTo(TimeUnit.SECONDS)
    .prefixedWith(prefix)
    .build(graphite)

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}

