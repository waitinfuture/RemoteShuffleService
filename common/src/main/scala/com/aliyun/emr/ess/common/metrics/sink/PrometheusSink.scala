package com.aliyun.emr.ess.common.metrics.sink

import java.util.Properties

import com.codahale.metrics.MetricRegistry

class PrometheusSink(val property: Properties, val registry: MetricRegistry) extends Sink {
  override def start(): Unit = { }

  override def stop(): Unit = { }

  override def report(): Unit = { }
}
