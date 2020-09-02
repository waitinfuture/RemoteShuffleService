package com.aliyun.emr.ess.common.metrics.source

import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, TimeUnit}

import scala.util.Random
import scala.collection.JavaConversions._

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.common.util.ThreadUtils
import com.aliyun.emr.ess.metrics.{EssHistogram, EssTimer, ResettableSlidingWindowReservoir}
import com.codahale.metrics._

case class NamedCounter(name: String, counter: Counter)

case class NamedGauge[T](name: String, gaurge: Gauge[T])

case class NamedHistogram(name: String, histogram: Histogram)

case class NamedTimer(name: String, timer: Timer)

abstract class AbstractSource(essConf: EssConf, role: String)
  extends Source with Logging {
  override val metricRegistry = new MetricRegistry()

  val slidingWindowSize: Int = EssConf.essMetricsSlidingWindowSize(essConf)

  val sampleRate: Double = EssConf.essMetricsSampleRate(essConf)

  final val InnerMetricsSize = EssConf.essInnerMetricsSize(essConf)
  val innerMetrics: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()

  val timerSupplier = new TimerSupplier(slidingWindowSize)

  val metricsClear = ThreadUtils.newDaemonSingleThreadExecutor(s"worker-metrics-clearer")

  protected val namedGauges: java.util.List[NamedGauge[_]] = new util.ArrayList[NamedGauge[_]]()

  def addGauge[T](name: String, f: Unit => T): Unit = {
    val supplier: MetricRegistry.MetricSupplier[Gauge[_]] = new GaugeSupplier[T](f)
    val gauge = metricRegistry.gauge(name, supplier)
    namedGauges.add(NamedGauge(name, gauge))
  }

  protected val namedTimers: ConcurrentHashMap[String, (NamedTimer, ConcurrentHashMap[String, Long])] =
    new ConcurrentHashMap[String, (NamedTimer, ConcurrentHashMap[String, Long])]()

  def addTimer(name: String): Unit = {
    val namedTimer = NamedTimer(name, metricRegistry.timer(name, timerSupplier))
    namedTimers.putIfAbsent(name, (namedTimer, new ConcurrentHashMap[String, Long]()))
  }

  protected val namedCounters: ConcurrentHashMap[String, NamedCounter] =
    new ConcurrentHashMap[String, NamedCounter]()

  def addCounter(name: String): Unit = {
    namedCounters.put(name, NamedCounter(name, metricRegistry.counter(name)))
  }

  protected def counters(): List[NamedCounter] = {
    namedCounters.values().toList
  }

  def gauges(): List[NamedGauge[_]] = {
    namedGauges.toList
  }

  protected def histograms(): List[NamedHistogram] = {
    List.empty[NamedHistogram]
  }

  protected def timers(): List[NamedTimer] = {
    namedTimers.values().map(_._1).toList
  }

  def needSample(): Boolean = {
    if (sampleRate >= 1) {
      true
    } else if (sampleRate <= 0) {
      false
    } else {
      Random.nextDouble() <= sampleRate
    }
  }

  override def sample[T](metricsName: String, key: String)(f: => T): T = {
    val sample = needSample()
    var r: Any = null
    try {
      if (sample) {
        doStartTimer(metricsName, key)
      }
      r = f
    } finally {
      if (sample) {
        doStopTimer(metricsName, key)
      }
    }
    r.asInstanceOf[T]
  }

  override def startTimer(metricsName: String, key: String): Unit = {
    if (needSample()) {
      doStartTimer(metricsName, key)
    }
  }

  override def stopTimer(metricsName: String, key: String): Unit = {
    doStopTimer(metricsName, key)
  }

  def doStartTimer(metricsName: String, key: String): Unit = {
    val pair = namedTimers.get(metricsName)
    if (pair != null) {
      pair._2.put(key, System.nanoTime())
    } else {
      logWarning(s"Metric $metricsName Not Found!")
    }
  }

  protected def doStopTimer(metricsName: String, key: String): Unit = {
    val (namedTimer, map) = namedTimers.get(metricsName)
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

  override def incCounter(metricsName: String, incV: Long = 1): Unit = {
    val counter = namedCounters.get(metricsName)
    if (counter != null) {
      counter.counter.inc(incV)
    } else {
      logWarning(s"Metric $metricsName Not Found!")
    }
  }

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

  protected def startCleaner(): Unit = {
    metricsClear.submit(new Runnable {
      override def run(): Unit = {
        while (true) {
          try {
            namedTimers.values().map(_._2).foreach(map => clearOldValues(map))
          } catch {
            case t: Throwable => logError(s"clearer quit with $t")
          } finally {
            Thread.sleep(600000 /** 10min **/)
          }
        }
      }
    })
  }

  private def updateInnerMetrics(str: String): Unit = {
    innerMetrics.synchronized {
      if (innerMetrics.size() >= InnerMetricsSize) {
        innerMetrics.remove()
      }
      innerMetrics.offer(str)
    }
  }

  def recordCounter(nc: NamedCounter): Unit = {
    val timestamp = System.currentTimeMillis()
    val sb = new StringBuilder
    sb.append(s"${normalizeKey(nc.name)}Count$label ${nc.counter.getCount} $timestamp\n")

    updateInnerMetrics(sb.toString())
  }

  def recordGauge(ng: NamedGauge[_]): Unit = {
    val timestamp = System.currentTimeMillis()
    val sb = new StringBuilder
    sb.append(s"${normalizeKey(ng.name)}Value$label ${ng.gaurge.getValue} $timestamp\n")

    updateInnerMetrics(sb.toString())
  }

  def recordHistogram(nh: NamedHistogram): Unit = {
    val timestamp = System.currentTimeMillis()
    val sb = new StringBuilder
    val metricName = nh.name
    val h = nh.histogram
    val snapshot = h.getSnapshot
    val prefix = normalizeKey(metricName)
    sb.append(s"${prefix}Count$label ${h.getCount} $timestamp\n")
    sb.append(s"${prefix}Max$label ${reportNanosAsMills(snapshot.getMax)} $timestamp\n")
    sb.append(s"${prefix}Mean$label ${reportNanosAsMills(snapshot.getMean)} $timestamp\n")
    sb.append(s"${prefix}Min$label ${reportNanosAsMills(snapshot.getMin)} $timestamp\n")
    sb.append(s"${prefix}50thPercentile$label ${reportNanosAsMills(snapshot.getMedian)} $timestamp\n")
    sb.append(s"${prefix}75thPercentile$label ${reportNanosAsMills(snapshot.get75thPercentile)} $timestamp\n")
    sb.append(s"${prefix}95thPercentile$label ${reportNanosAsMills(snapshot.get95thPercentile)} $timestamp\n")
    sb.append(s"${prefix}98thPercentile$label ${reportNanosAsMills(snapshot.get98thPercentile)} $timestamp\n")
    sb.append(s"${prefix}99thPercentile$label ${reportNanosAsMills(snapshot.get99thPercentile)} $timestamp\n")
    sb.append(s"${prefix}999thPercentile$label ${reportNanosAsMills(snapshot.get999thPercentile)} $timestamp\n")

    updateInnerMetrics(sb.toString())
  }

  def recordTimer(nt: NamedTimer): Unit = {
    val timestamp = System.currentTimeMillis()
    val sb = new StringBuilder
    val snapshot = nt.timer.getSnapshot
    val prefix = normalizeKey(nt.name)
    sb.append(s"${prefix}Count$label ${nt.timer.getCount} $timestamp\n")
    sb.append(s"${prefix}Max$label ${reportNanosAsMills(snapshot.getMax)} $timestamp\n")
    sb.append(s"${prefix}Mean$label ${reportNanosAsMills(snapshot.getMean)} $timestamp\n")
    sb.append(s"${prefix}Min$label ${reportNanosAsMills(snapshot.getMin)} $timestamp\n")
    sb.append(s"${prefix}50thPercentile$label ${reportNanosAsMills(snapshot.getMedian)} $timestamp\n")
    sb.append(s"${prefix}75thPercentile$label ${reportNanosAsMills(snapshot.get75thPercentile)} $timestamp\n")
    sb.append(s"${prefix}95thPercentile$label ${reportNanosAsMills(snapshot.get95thPercentile)} $timestamp\n")
    sb.append(s"${prefix}98thPercentile$label ${reportNanosAsMills(snapshot.get98thPercentile)} $timestamp\n")
    sb.append(s"${prefix}99thPercentile$label ${reportNanosAsMills(snapshot.get99thPercentile)} $timestamp\n")
    sb.append(s"${prefix}999thPercentile$label ${reportNanosAsMills(snapshot.get999thPercentile)} $timestamp\n")

    updateInnerMetrics(sb.toString())
  }

  override def getMetrics(): String = {
    counters().foreach(c => recordCounter(c))
    gauges().foreach(g => recordGauge(g))
    histograms().foreach(h => {
      recordHistogram(h)
      h.asInstanceOf[EssHistogram].getReservoir()
        .asInstanceOf[ResettableSlidingWindowReservoir].reset()
    })
    timers().foreach(t => {
      recordTimer(t)
      t.timer.asInstanceOf[EssTimer].getReservoir()
        .asInstanceOf[ResettableSlidingWindowReservoir].reset()
    })
    val sb = new StringBuilder
    innerMetrics.synchronized {
      while (!innerMetrics.isEmpty) {
        sb.append(innerMetrics.poll())
      }
      innerMetrics.clear()
    }
    sb.toString()
  }

  protected def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}_"
  }

  protected def reportNanosAsMills(value: Double): Double = {
    BigDecimal(value / 1000000).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
  
  val label = s"""{role="$role"}"""
}

class TimerSupplier(val slidingWindowSize: Int)
  extends MetricRegistry.MetricSupplier[Timer] {
    override def newMetric(): Timer = {
      new EssTimer(new ResettableSlidingWindowReservoir(slidingWindowSize))
    }
}

class GaugeSupplier[T](f: Unit => T) extends MetricRegistry.MetricSupplier[Gauge[_]] {
  override def newMetric(): Gauge[T] = {
    new Gauge[T] {
      override def getValue: T = f()
    }
  }
}