package com.aliyun.emr.ess.common.metrics.source

import com.codahale.metrics.MetricRegistry

trait Source {
  def sourceName: String
  def metricRegistry: MetricRegistry
  def sample[T](metricsName: String, key: String)(f: => T): T
  def startTimer(metricsName: String, key: String): Unit
  def stopTimer(metricsName: String, key: String): Unit
  def incCounter(metricsName: String, incV: Long): Unit
  def getMetrics(): String
}
