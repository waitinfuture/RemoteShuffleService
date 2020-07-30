package com.aliyun.emr.ess.service.deploy.worker

import java.io.{File, FileNotFoundException}
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.LinkedBlockingQueue
import java.util.function.IntUnaryOperator

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.protocol.PartitionLocation

private[worker] case class WriteTask(
    buffer: ByteBuffer,
    fileChannel: FileChannel,
    counter: AtomicInteger)

private[worker] final class DiskFlusher(index: Int, queueCapacity: Int, bufferSize: Int) {
  private val workingQueue = new LinkedBlockingQueue[WriteTask](queueCapacity)
  private val bufferQueue = new LinkedBlockingQueue[ByteBuffer](queueCapacity)
  for (_ <- 0 until queueCapacity) {
    bufferQueue.put(ByteBuffer.allocateDirect(bufferSize))
  }

  private val worker = new Thread(s"DiskFlusher-$index") {
    override def run(): Unit = {
      while (true) {
        val task = workingQueue.take()
        task.fileChannel.write(task.buffer)
        task.buffer.clear()
        bufferQueue.put(task.buffer)
        task.counter.decrementAndGet()
      }
    }
  }
  worker.setDaemon(true)
  worker.start()

  def takeBuffer(): ByteBuffer = {
    bufferQueue.take()
  }

  def addTask(task: WriteTask): Unit = {
    workingQueue.put(task)
  }
}

private[worker] final class LocalStorageManager(conf: EssConf) extends Logging {

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

  private val diskFlushers = {
    val queueCapacity = EssConf.essWorkerFlushQueueCapacity(conf)
    val flushBufferSize = EssConf.essWorkerFlushBufferSize(conf).toInt
    (1 to baseDirs.length).map(i => new DiskFlusher(i, queueCapacity, flushBufferSize)).toArray
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

  private def getNextIndex() = counter.getAndUpdate(counterOperator)

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
    val index = getNextIndex()
    val shuffleDir = new File(baseDirs(index), s"$appId/$shuffleId")
    shuffleDir.mkdirs()
    val fileName = s"$reduceId-$epoch-${mode.mode()}"
    new FileWriter(new File(shuffleDir, fileName), diskFlushers(index))
  }
}

private object LocalStorageManager {
  private val workingDir = "hadoop/ess-worker/data"
}
