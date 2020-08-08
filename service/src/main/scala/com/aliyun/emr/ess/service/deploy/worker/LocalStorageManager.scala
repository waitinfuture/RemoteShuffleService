package com.aliyun.emr.ess.service.deploy.worker

import java.io.{File, IOException}
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import java.util.function.IntUnaryOperator

import scala.collection.JavaConversions._

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.ess.protocol.PartitionLocation

private[worker] case class FlushTask(
    buffer: ByteBuffer,
    fileChannel: FileChannel,
    notifier: FileWriter.FlushNotifier)

private[worker] final class DiskFlusher(
    index: Int, queueCapacity: Int, bufferSize: Int) extends Logging {
  private val workingQueue = new LinkedBlockingQueue[FlushTask](queueCapacity)
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
              logError(s"[DiskFlusher] write failed ${e.getMessage}")
              task.notifier.setException(e)
          }
        }

        returnBuffer(task.buffer)
        task.notifier.numPendingFlushes.decrementAndGet()
      }
    }
  }
  worker.setDaemon(true)
  worker.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit = {
      logError(s"${t.getName} thread terminated, worker exits", e)
      System.exit(1)
    }
  })
  worker.start()

  def takeBuffer(timeoutMs: Long): ByteBuffer = {
    bufferQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
  }

  def returnBuffer(buffer: ByteBuffer): Unit = {
    buffer.clear()
    bufferQueue.put(buffer)
  }

  def addTask(task: FlushTask): Unit = {
    workingQueue.put(task)
  }

  def bufferQueueInfo(): String = s"${worker.getName} available buffers: ${bufferQueue.size()}"
}

private[worker] final class LocalStorageManager(conf: EssConf) extends Logging {

  import LocalStorageManager._

  private val workingDirs = {
    val baseDirs = EssConf.essWorkerBaseDirs(conf)
    baseDirs.map(new File(_, workingDirName))
  }

  workingDirs.foreach { dir =>
    dir.mkdirs()
  }

  private val diskFlushers = {
    val queueCapacity = EssConf.essWorkerFlushQueueCapacity(conf)
    val flushBufferSize = EssConf.essWorkerFlushBufferSize(conf).toInt
    (1 to workingDirs.length).map(i => new DiskFlusher(i, queueCapacity, flushBufferSize)).toArray
  }

  private val fetchChunkSize = EssConf.essWorkerFetchChunkSize(conf)
  private val timeoutMs = EssConf.essFlushTimeoutMs(conf)

  private val counter = new AtomicInteger()
  private val counterOperator = new IntUnaryOperator() {
    override def applyAsInt(operand: Int): Int = (operand + 1) % workingDirs.length
  }

  // shuffleKey -> (fileName -> writer)
  private val writers =
    new ConcurrentHashMap[String, ConcurrentHashMap[String, FileWriter]]()

  private def deleteDirectory(dir: File): Boolean = {
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

  @throws[IOException]
  def createWriter(appId: String, shuffleId: Int, location: PartitionLocation): FileWriter = {
    createWriter(appId, shuffleId, location.getReduceId, location.getEpoch, location.getMode)
  }

  @throws[IOException]
  def createWriter(
      appId: String,
      shuffleId: Int,
      reduceId: Int,
      epoch: Int,
      mode: PartitionLocation.Mode): FileWriter = {
    val index = getNextIndex()
    val shuffleDir = new File(workingDirs(index), s"$appId/$shuffleId")
    val fileName = s"$reduceId-$epoch-${mode.mode()}"
    val file = new File(shuffleDir, fileName)
    shuffleDir.mkdirs()
    file.createNewFile()
    val writer = new FileWriter(file, diskFlushers(index), fetchChunkSize, timeoutMs)

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

  def shuffleKeySet(): util.Set[String] = writers.keySet()

  def cleanup(expiredShuffleKeys: util.HashSet[String]): Unit = {
    expiredShuffleKeys.foreach { shuffleKey =>
      logInfo(s"cleanup $shuffleKey")
      writers.remove(shuffleKey)
      val splits = shuffleKey.split("-")
      val appId = splits.dropRight(1).mkString("-")
      val shuffleId = splits.last
      workingDirs.foreach { workingDir =>
        val dir = new File(workingDir, s"$appId/$shuffleId")
        deleteDirectory(dir)
      }
    }
  }

  private val cleanupEmptyAppDirsScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("cleanup-empty-dirs-scheduler")
  cleanupEmptyAppDirsScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      cleanupEmptyAppDirs()
    }
  }, 30, 30, TimeUnit.MINUTES)

  private def cleanupEmptyAppDirs(): Unit = {
    val expireTime = System.currentTimeMillis() - expireDurationMs
    workingDirs.foreach { workingDir =>
      val appDirs = workingDir.listFiles
      if (appDirs != null) {
        for (appDir <- appDirs if appDir.lastModified() < expireTime) {
          appDir.delete()
        }
      }
    }
  }

  def logAvailableFlushBuffersInfo(): Unit =
    diskFlushers.foreach(f => logInfo(f.bufferQueueInfo()))
}

private object LocalStorageManager {
  private val workingDirName = "hadoop/ess-worker/data"
  private val expireDurationMs = TimeUnit.HOURS.toMillis(2)
}
