package com.aliyun.emr.ess.service.deploy.worker

import java.io.{File, FileNotFoundException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntUnaryOperator

import scala.util.Random

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.protocol.PartitionLocation

private[worker] class LocalStorageManager(conf: EssConf) extends Logging {

  import LocalStorageManager._

  private val baseDirs = {
    val prefix = EssConf.essWorkerBaseDirPrefix(conf)
    val number = EssConf.essWorkerBaseDirNumber(conf)
    (1 to number).map(i => new File(s"$prefix$i", workingDir)).toArray
  }
  baseDirs.foreach { dir =>
    if (dir.exists()) {
      deleteDirectory(dir)
    }
    dir.mkdirs()
  }

  private val counter = new AtomicInteger()
  private val counterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = (operand + 1) % baseDirs.length
  }

  def deleteDirectory(dir: File): Boolean = {
    val allContents = dir.listFiles
    if (allContents != null) {
      for (file <- allContents) {
        deleteDirectory(file)
      }
    }
    dir.delete()
  }

  private def getNextBaseDir() = baseDirs(counter.getAndUpdate(counterOperator))

  @throws[FileNotFoundException]
  def getWriter(appId: String, shuffleId: Int, location: PartitionLocation): FileWriter = {
    getWriter(appId, shuffleId, location.getReduceId, location.getEpoch, location.getMode)
  }

  @throws[FileNotFoundException]
  def getWriter(
      appId: String,
      shuffleId: Int,
      reduceId: Int,
      epoch: Int,
      mode: PartitionLocation.Mode): FileWriter = {
    val baseDir = getNextBaseDir()
    val shuffleDir = new File(baseDir, s"$appId/$shuffleId")
    shuffleDir.mkdirs()
    val fileName = s"$reduceId-$epoch-${mode.mode()}"
    new FileWriter(new File(shuffleDir, fileName))
  }
}

private object LocalStorageManager {
  private val workingDir = "hadoop/ess-worker/data"
}
