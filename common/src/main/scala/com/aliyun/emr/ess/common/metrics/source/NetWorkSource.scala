package com.aliyun.emr.ess.common.metrics.source

import com.aliyun.emr.ess.common.EssConf

class NetWorkSource(essConf: EssConf, role: String) extends AbstractSource(essConf, role) {
  override val sourceName = s"network"

  import NetWorkSource._
  // add timer
  addTimer(FetchChunkTime)

  // start cleaner
  startCleaner()
}

object NetWorkSource {
  val FetchChunkTime = "FetchChunkTime"
}
