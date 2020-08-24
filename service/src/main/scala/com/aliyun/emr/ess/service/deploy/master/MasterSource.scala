package com.aliyun.emr.ess.service.deploy.master

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.metrics.source.{AbstractSource, NamedCounter, NamedTimer}
import com.aliyun.emr.ess.common.util.ThreadUtils
import com.codahale.metrics.MetricRegistry

class MasterSource(essConf: EssConf) extends AbstractSource(essConf) with Logging{
  override val metricRegistry = new MetricRegistry()
  override val sourceName = s"master"

  val COMMIT_FILES_TIME = {
    val name = MetricRegistry.name(s"commitFileTime")
    NamedTimer(name, metricRegistry.timer(name, timerSupplier))
  }

  val RESERVE_BUFFER_TIME = {
    val name = MetricRegistry.name(s"reserveBufferTime")
    NamedTimer(name, metricRegistry.timer(name, timerSupplier))
  }

  val REVIVE_TIME = {
    val name = MetricRegistry.name(s"reviveTime")
    NamedTimer(name, metricRegistry.timer(name, timerSupplier))
  }

  val RPC_FAILURE_COUNTER = {
    val name = MetricRegistry.name("failureCounter")
    NamedCounter(name, metricRegistry.counter(name))
  }

  val COMMIT_FILE_TIMER_MAP = new ConcurrentHashMap[String, Long]()
  val RESERVE_BUFFER_TIMER_MAP = new ConcurrentHashMap[String, Long]()
  val REVIVE_TIMER_MAP = new ConcurrentHashMap[String, Long]()

  override def counters(): List[NamedCounter] = {
    List(RPC_FAILURE_COUNTER)
  }

  override def timers(): List[NamedTimer] = {
    List(COMMIT_FILES_TIME, RESERVE_BUFFER_TIME, REVIVE_TIME)
  }

  def getTimerAndMap(metricsName: String): (NamedTimer, ConcurrentHashMap[String, Long]) = {
    metricsName match {
      case MasterSource.COMMIT_FILES_TIME => (COMMIT_FILES_TIME, COMMIT_FILE_TIMER_MAP)
      case MasterSource.RESERVE_BUFFER_TIME => (RESERVE_BUFFER_TIME, RESERVE_BUFFER_TIMER_MAP)
      case MasterSource.REVIVE_TIME => (REVIVE_TIME, REVIVE_TIMER_MAP)
      case _ => throw new Exception(s"Unkown metrics name $metricsName")
    }
  }

  override def doStartTimer(metricsName: String, key: String): Unit = {
    metricsName match {
      case MasterSource.COMMIT_FILES_TIME =>
        COMMIT_FILE_TIMER_MAP.put(key, System.nanoTime())

      case MasterSource.RESERVE_BUFFER_TIME =>
        RESERVE_BUFFER_TIMER_MAP.put(key, System.nanoTime())

      case MasterSource.REVIVE_TIME =>
        REVIVE_TIMER_MAP.put(key, System.nanoTime())

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
      case MasterSource.RPC_FAILURE_COUNT =>
        RPC_FAILURE_COUNTER.counter.inc(incV)

      case _ =>
    }
  }

  override def decCounter(metricsName: String, decV: Long = 1) = {
    metricsName match {
      case MasterSource.RPC_FAILURE_COUNT =>
        RPC_FAILURE_COUNTER.counter.dec(decV)

      case _ =>
    }
  }

  val metricsClear = ThreadUtils.newDaemonSingleThreadExecutor(s"master-metrics-clearer")
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
          clearOldValues(REVIVE_TIMER_MAP)
          clearOldValues(RESERVE_BUFFER_TIMER_MAP)
        } catch {
          case t: Throwable => logError(s"clearer quit with $t")

        } finally {
          Thread.sleep(600000 /** 10min **/)
        }
      }
    }
  })

}

object MasterSource {
  val COMMIT_FILES_TIME = "COMMIT_FILES_TIME"

  val RESERVE_BUFFER_TIME = "RESERVE_BUFFER_TIME"

  val REVIVE_TIME = "REVIVE_TIME"

  val BLACKLISTED_WORKER_COUNT = "BLACKLISTED_WORKER_COUNTER"

  val REGISTERED_SHUFFLE_COUNT = "REGISTERED_SHUFFLE_COUNTER"

  val RPC_FAILURE_COUNT = "RPC_FAILURE_COUNTER"

  val SHUFFLE_MANAGER_HOSTNAME_LIST = "SHUFFLE_MANAGER_HOSTNAME_LIST"
}
