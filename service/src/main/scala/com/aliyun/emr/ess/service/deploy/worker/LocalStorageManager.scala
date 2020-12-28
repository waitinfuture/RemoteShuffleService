package com.aliyun.emr.ess.service.deploy.worker

import java.io.{File, IOException}
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import java.util.function.IntUnaryOperator

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.ess.common.metrics.source.AbstractSource
import com.aliyun.emr.ess.protocol.PartitionLocation

private[worker] case class FlushTask(
    buffer: ByteBuffer,
    fileChannel: FileChannel,
    notifier: FileWriter.FlushNotifier)

private[worker] final class DiskFlusher(
    index: Int, queueCapacity: Int, bufferSize: Int, workerSource: AbstractSource) extends Logging {
  private val workingQueue = new LinkedBlockingQueue[FlushTask](queueCapacity)
  private val bufferQueue = new LinkedBlockingQueue[ByteBuffer](queueCapacity)
  for (_ <- 0 until queueCapacity) {
    bufferQueue.put(ByteBuffer.allocateDirect(bufferSize))
  }

  private val worker = new Thread(s"DiskFlusher-$index") {
    override def run(): Unit = {
      while (true) {
        val task = workingQueue.take()

        val key = s"DiskFlusher-$index"
        workerSource.sample(WorkerSource.FlushDataTime, key) {
          if (!task.notifier.hasException) {
            try {
              task.fileChannel.write(task.buffer)
            } catch {
              case e: IOException =>
                logError(s"[DiskFlusher] write failed ${e.getMessage}")
                task.notifier.setException(e)
            }
          }

        // return pre-allocated buffer to bufferQueue
        if (task.buffer.capacity() == bufferSize) {
          returnBuffer(task.buffer)
        }
        task.notifier.numPendingFlushes.decrementAndGet()
        }
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

  def addTask(task: FlushTask, timeoutMs: Long): Boolean = {
    workingQueue.offer(task, timeoutMs, TimeUnit.MILLISECONDS)
  }

  def bufferQueueInfo(): String = s"${worker.getName} available buffers: ${bufferQueue.size()}"
}

private[worker] final class LocalStorageManager(conf: EssConf, workerSource: AbstractSource) extends Logging {

  import LocalStorageManager._

  private val workingDirs: Array[File] = {
    val baseDirs = EssConf.essWorkerBaseDirs(conf).map(new File(_, workingDirName))
    var availableDirs = new mutable.HashSet[File]()
    baseDirs.foreach { dir =>
      try {
        dir.mkdirs()
        // since mkdirs do not throw any exception,
        // we should check directory status by create a test file
        val file = new File(dir, s"_SUCCESS_${System.currentTimeMillis()}")
        file.createNewFile()
        file.delete()
        availableDirs += dir
      } catch {
        case ie: IOException =>
          if (EssConf.essWorkerRemoveUnavailableDirs(conf)) {
            logWarning(s"Exception raised when trying to create a file in dir $dir, due to " +
              "`ess.worker.unavailable.dirs.remove` is true, remove it.")
          } else {
            throw ie
          }
      }
    }
    if (availableDirs.size <= 0) {
      throw new IOException("No available working directory.")
    }
    availableDirs.toArray
  }

  private val diskFlushers = {
    val queueCapacity = EssConf.essWorkerFlushQueueCapacity(conf)
    val flushBufferSize = EssConf.essWorkerFlushBufferSize(conf).toInt
    (1 to workingDirs.length).map(i => new DiskFlusher(i, queueCapacity, flushBufferSize, workerSource)).toArray
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

  def numDisks: Int = workingDirs.length

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
    val writer = new FileWriter(file, diskFlushers(index), fetchChunkSize, timeoutMs, workerSource)

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

  def cleanupExpiredShuffleKey(expiredShuffleKeys: util.HashSet[String]): Unit = {
    expiredShuffleKeys.foreach { shuffleKey =>
      logInfo(s"Cleanup $shuffleKey")
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

  private val noneEmptyDirExpireDurationMs = EssConf.essNoneEmptyDirExpireDurationMs(conf)
  private val noneEmptyDirCleanUpThreshold = EssConf.essNoneEmptyDirCleanUpThreshold(conf)
  private val emptyDirExpireDurationMs = EssConf.essEmptyDirExpireDurationMs(conf)
  private val cleanupExpiredAppDirsScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("cleanup-expired-dirs-scheduler")
  cleanupExpiredAppDirsScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = {
      cleanupExpiredAppDirs()
    }
  }, 30, 30, TimeUnit.MINUTES)

  private def cleanupExpiredAppDirs(deleteRecursively: Boolean = false, expireDuration: Long): Unit = {
    workingDirs.foreach { workingDir =>
      var appDirs = workingDir.listFiles

      if (appDirs != null) {
        if (deleteRecursively) {
          appDirs = appDirs.sortBy(_.lastModified()).take(noneEmptyDirCleanUpThreshold)

          for (appDir <- appDirs if appDir.lastModified() < expireDuration) {
            deleteDirectory(appDir)
          }
        } else {
          for (appDir <- appDirs if appDir.lastModified() < expireDuration) {
            appDir.delete()
          }
        }
      }
    }
  }

  private def cleanupExpiredAppDirs(): Unit = {
    // Clean up empty dirs, since the appDir do not delete during expired shuffle key cleanup
    cleanupExpiredAppDirs(expireDuration = System.currentTimeMillis() - emptyDirExpireDurationMs)

    // Clean up non-empty dirs which has not been modified
    // in the past {{noneEmptyExpireDurationsMs}}, since
    // non empty dirs may exist after cluster restart.
    cleanupExpiredAppDirs(
      true, System.currentTimeMillis() - noneEmptyDirExpireDurationMs)
  }

  def logAvailableFlushBuffersInfo(): Unit =
    diskFlushers.foreach(f => logInfo(f.bufferQueueInfo()))
}

private object LocalStorageManager {
  private val workingDirName = "hadoop/ess-worker/data"
}
