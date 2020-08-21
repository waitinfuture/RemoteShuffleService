package com.aliyun.emr.ess.service.deploy.worker

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.metrics.source.{AbstractSource, NamedCounter, NamedTimer}
import com.aliyun.emr.ess.common.util.ThreadUtils
import com.codahale.metrics.MetricRegistry

class WorkerSource(essConf: EssConf) extends AbstractSource(essConf) with Logging {
  override val metricRegistry = new MetricRegistry()
  override val sourceName = s"worker"

  val COMMIT_FILES_TIME = {
    val name = MetricRegistry.name(s"commitFileTime")
    NamedTimer(name, metricRegistry.timer(name, timerSupplier))
  }

  val RESERVE_BUFFER_TIME = {
    val name = MetricRegistry.name(s"reserveBufferTime")
    NamedTimer(name, metricRegistry.timer(name, timerSupplier))
  }

  val FLUSH_DATA_TIME = {
    val name = MetricRegistry.name(s"flushDataTime")
    NamedTimer(name, metricRegistry.timer(name, timerSupplier))
  }

  val PUSH_DATA_TIME = {
    val name = MetricRegistry.name(s"pushDataTime")
    NamedTimer(name, metricRegistry.timer(name, timerSupplier))
  }

  val RPC_FAILURE_COUNTER = {
    val name = MetricRegistry.name(s"failureCounter")
    NamedCounter(name, metricRegistry.counter(name))
  }

  val COMMIT_FILE_TIMER_MAP     = new ConcurrentHashMap[String, Long]()
  val PUSH_DATA_TIMER_MAP       = new ConcurrentHashMap[String, Long]()
  val RESERVE_BUFFER_TIMER_MAP  = new ConcurrentHashMap[String, Long]()
  val FLUSH_DATA_TIMER_MAP      = new ConcurrentHashMap[String, Long]()

  def getTimerAndMap(metricsName: String): (NamedTimer, ConcurrentHashMap[String, Long]) = {
    metricsName match {
      case WorkerSource.COMMIT_FILES_TIME => (COMMIT_FILES_TIME, COMMIT_FILE_TIMER_MAP)
      case WorkerSource.FLUSH_DATA_TIME => (FLUSH_DATA_TIME, FLUSH_DATA_TIMER_MAP)
      case WorkerSource.PUSH_DATA_TIME => (PUSH_DATA_TIME, PUSH_DATA_TIMER_MAP)
      case WorkerSource.RESERVE_BUFFER_TIME => (RESERVE_BUFFER_TIME, RESERVE_BUFFER_TIMER_MAP)
      case _ => throw new Exception(s"Unkown metrics name $metricsName")
    }
  }

  override def counters(): List[NamedCounter] = {
    List(RPC_FAILURE_COUNTER)
  }

  override def timers: List[NamedTimer] = {
    List(COMMIT_FILES_TIME, RESERVE_BUFFER_TIME, FLUSH_DATA_TIME, PUSH_DATA_TIME)
  }

  override def doStartTimer(metricsName: String, key: String): Unit = {
    metricsName match {
      case WorkerSource.COMMIT_FILES_TIME =>
        COMMIT_FILE_TIMER_MAP.put(key, System.nanoTime())

      case WorkerSource.PUSH_DATA_TIME =>
        PUSH_DATA_TIMER_MAP.put(key, System.nanoTime())

      case WorkerSource.RESERVE_BUFFER_TIME =>
        RESERVE_BUFFER_TIMER_MAP.put(key, System.nanoTime())

      case WorkerSource.FLUSH_DATA_TIME =>
        FLUSH_DATA_TIMER_MAP.put(key, System.nanoTime())

      case _ =>
    }
  }

  override def doStopTimer(metricsName: String, key: String): Unit = {
    val (namedTimer, map) = getTimerAndMap(metricsName)
    val startTime = Option(map.remove(key))
    startTime match {
      case Some(t) =>
        namedTimer.timer.update(System.nanoTime() - t, TimeUnit.NANOSECONDS)
        if (namedTimer.timer.getCount % slidingWindowSize == 0) {
          recordTimer(namedTimer)
        }
      case None =>
    }
  }

  override def incCounter(metricsName: String, incV: Long = 1) = {
    metricsName match {
      case WorkerSource.RPC_FAILURE_COUNTER =>
        RPC_FAILURE_COUNTER.counter.inc(incV)

      case _ =>
    }
  }

  override def decCounter(metricsName: String, decV: Long = 1) = {
    metricsName match {
      case WorkerSource.RPC_FAILURE_COUNTER =>
        RPC_FAILURE_COUNTER.counter.dec(decV)

      case _ =>
    }
  }

  val metricsClear = ThreadUtils.newDaemonSingleThreadExecutor(s"worker-metrics-clearer")

  private def clearOldValues(map: ConcurrentHashMap[String, Long]): Unit = {
    if (map.size > 5000) {
      // remove values has existed more than 15 min
      // 50000 values may be 1MB more or less
      val threshTime = System.nanoTime() - 900000000000L
      val it = map.entrySet().iterator
      while (it.hasNext) {
        val entry = it.next()
        if (entry.getValue < threshTime) {
          it.remove()
        }
      }
    }
  }

  metricsClear.submit(new Runnable {
    override def run(): Unit = {
      while (true) {
        try {
          clearOldValues(COMMIT_FILE_TIMER_MAP)
          clearOldValues(PUSH_DATA_TIMER_MAP)
          clearOldValues(RESERVE_BUFFER_TIMER_MAP)
          clearOldValues(FLUSH_DATA_TIMER_MAP)
        } catch {
          case t: Throwable => logError(s"clearer quit with $t")

        } finally {
          Thread.sleep(600000 /** 10min **/)
        }
      }
    }
  })
}

object WorkerSource {
  val COMMIT_FILES_TIME = "COMMIT_FILES_TIME"

  val RESERVE_BUFFER_TIME = "RESERVE_BUFFER_TIME"

  val FLUSH_DATA_TIME = "FLUSH_DATA_TIME"

  val PUSH_DATA_TIME = "PUSH_DATA_TIME"

  val RPC_FAILURE_COUNTER = "RPC_FAILURE_COUNTER"

  val REGISTERED_SHUFFLE_COUNT = "REGISTERED_SHUFFLE_COUNTER"
}
