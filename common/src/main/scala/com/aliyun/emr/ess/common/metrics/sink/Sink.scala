package com.aliyun.emr.ess.common.metrics.sink

trait Sink {
  def start(): Unit

  def stop(): Unit

  def report(): Unit
}
