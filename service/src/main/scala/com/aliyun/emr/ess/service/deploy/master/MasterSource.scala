package com.aliyun.emr.ess.service.deploy.master

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.metrics.source.AbstractSource
import com.aliyun.emr.ess.common.metrics.MetricsSystem

class MasterSource(essConf: EssConf) extends AbstractSource(essConf, MetricsSystem.ROLE_MASTER) with Logging{
  override val sourceName = s"master"

  import MasterSource._
  // add counter
  addCounter(MasterRPCFailureCount)

  // add Timer
  addTimer(CommitFilesTime)
  addTimer(ReserveBufferTime)
  addTimer(ReviveTime)

  // start cleaner
  startCleaner()
}

object MasterSource {
  val ServletPath = "/metrics/prometheus"

  val CommitFilesTime = "CommitFilesTime"

  val ReserveBufferTime = "ReserveBufferTime"

  val ReviveTime = "ReviveTime"

  val BlacklistedWorkerCount = "BlacklistedWorkerCount"

  val RegisteredShuffleCount = "RegisteredShuffleCount"

  val MasterRPCFailureCount = "MasterRPCFailureCount"
}
