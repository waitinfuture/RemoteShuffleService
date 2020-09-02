package com.aliyun.emr.ess.service.deploy.worker

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.metrics.source.AbstractSource
import com.aliyun.emr.ess.common.metrics.MetricsSystem

class WorkerSource(essConf: EssConf) extends AbstractSource(essConf, MetricsSystem.ROLE_WOKRER) with Logging {
  override val sourceName = "worker"

  import WorkerSource._
  //add counters
  addCounter(RPCFailureCount)
  addCounter(PushDataFailCount)

  // add Timers
  addTimer(CommitFilesTime)
  addTimer(ReserveBufferTime)
  addTimer(FlushDataTime)
  addTimer(ReplicateTime)
  addTimer(MasterPushDataTime)
  addTimer(SlavePushDataTime)
  addTimer(CreateClientTime)
  addTimer(PushDataWriteTime)
  addTimer(FileWriterInitTakeBufferTime)

  // start cleaner thread
  startCleaner()
}

object WorkerSource {
  val ServletPath = "/metrics/prometheus"

  val CommitFilesTime = "CommitFilesTime"

  val ReserveBufferTime = "ReserveBufferTime"

  val FlushDataTime = "FlushDataTime"

  // push data
  val ReplicateTime = "ReplicateTime"
  val MasterPushDataTime = "MasterPushDataTime"
  val SlavePushDataTime = "SlavePushDataTime"
  val CreateClientTime = "CreateClientTime"
  val PushDataFailCount = "PushDataFailCount"
  val PushDataWriteTime = "PushDataWriteTime"

  // flush
  val FileWriterInitTakeBufferTime = "FileWriterInitTakeBufferTime"

  val RPCFailureCount = "RPCFailCount"

  val RegisteredShuffleCount = "RegisteredShuffleCount"

  // slots
  val TotalSlots = "TotalSlots"
  val SlotsUsed = "SlotsUsed"
  val SlotsAvailable = "SlotsAvailable"
}
