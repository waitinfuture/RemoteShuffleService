package com.aliyun.emr.ess.service.deploy.worker

import java.io.{File, FileNotFoundException, IOException}
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.function.IntUnaryOperator

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.util.Utils
import com.aliyun.emr.ess.protocol.PartitionLocation

private[worker] case class WriteTask(
    buffer: ByteBuffer,
    fileChannel: FileChannel,
    notifier: FileWriter.FlushNotifier)

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

        if (!task.notifier.hasException) {
          try {
            task.fileChannel.write(task.buffer)
          } catch {
            case e: IOException =>
              task.notifier.setException(e)
          }
        }

        returnBuffer(task.buffer)
        task.notifier.numPendingFlushes.decrementAndGet()
      }
    }
  }
  worker.setDaemon(true)
  worker.start()

  def takeBuffer(): ByteBuffer = {
    bufferQueue.take()
  }

  def returnBuffer(buffer: ByteBuffer): Unit = {
    buffer.clear()
    bufferQueue.put(buffer)
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

  private val fetchChunkSize = EssConf.essWorkerFetchChunkSize(conf)

  private val counter = new AtomicInteger()
  private val counterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = (operand + 1) % baseDirs.length
  }

  // shuffleKey -> (fileName -> writer)
  private val writers =
    new ConcurrentHashMap[String, ConcurrentHashMap[String, FileWriter]]()

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

  private val newMapFunc =
    new java.util.function.Function[String, ConcurrentHashMap[String, FileWriter]]() {
      override def apply(key: String): ConcurrentHashMap[String, FileWriter] =
        new ConcurrentHashMap()
    }

  @throws[FileNotFoundException]
  def createWriter(appId: String, shuffleId: Int, location: PartitionLocation): FileWriter = {
    createWriter(appId, shuffleId, location.getReduceId, location.getEpoch, location.getMode)
  }

  @throws[FileNotFoundException]
  def createWriter(
      appId: String,
      shuffleId: Int,
      reduceId: Int,
      epoch: Int,
      mode: PartitionLocation.Mode): FileWriter = {
    val index = getNextIndex()
    val shuffleDir = new File(baseDirs(index), s"$appId/$shuffleId")
    shuffleDir.mkdirs()
    val fileName = s"$reduceId-$epoch-${mode.mode()}"
    val writer = new FileWriter(new File(shuffleDir, fileName), diskFlushers(index), fetchChunkSize)

    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
    val shuffleMap = writers.computeIfAbsent(shuffleKey, newMapFunc)
    shuffleMap.put(fileName, writer)
    writer
  }

  def getWriter(shuffleKey: String, fileName: String): FileWriter = {
    val shuffleMap = writers.get(shuffleKey)
    if (shuffleMap ne null) {
      shuffleMap.get(fileName)
    } else {
      null
    }
  }

  def cleanup(shuffleKey: String): Unit = {
    val shuffleMap = writers.remove(shuffleKey)
    if (shuffleMap ne null) {
      // TODO
    }
  }
}

private object LocalStorageManager {
  private val workingDir = "hadoop/ess-worker/data"
}
