package com.aliyun.emr.ess.common.metrics.source

import com.aliyun.emr.ess.common.EssConf
import com.codahale.metrics.MetricRegistry

class NetWorkSource(essConf: EssConf) extends AbstractSource(essConf) {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = s"network"

  val FETCH_CHUNK_TIMER = {
    val name = MetricRegistry.name(s"fetchChunkTime")
    NamedTimer(name, metricRegistry.timer(name, timerSupplier))
  }
}
