package com.aliyun.emr.ess.common.metrics.source

import java.util
import java.util.concurrent.ConcurrentLinkedQueue

import scala.util.Random
import scala.collection.JavaConversions._

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.internal.Logging
import com.aliyun.emr.ess.metrics.{EssHistogram, EssTimer, ResettableSlidingWindowReservoir}
import com.codahale.metrics._

case class NamedCounter(name: String, counter: Counter)

case class NamedGauge[T](name: String, gaurge: Gauge[T])

case class NamedHistogram(name: String, histogram: Histogram)

case class NamedTimer(name: String, timer: Timer)

abstract class AbstractSource(essConf: EssConf)
  extends Source with Logging {
  val slidingWindowSize: Int = EssConf.essMetricsSlidingWindowSize(essConf)

  val sampleRate: Double = EssConf.essMetricsSampleRate(essConf)

  final val InnerMetricsSize = EssConf.essInnerMetricsSize(essConf)
  val innerMetrics: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()

  val timerSupplier = new TimerSupplier(slidingWindowSize)

  private val namedGauges: java.util.List[NamedGauge[_]] = new util.ArrayList[NamedGauge[_]]()

  def addGauge[T](name: String, f: Unit => T): Unit = {
    val supplier: MetricRegistry.MetricSupplier[Gauge[_]] = new GaugeSupplier[T](f)
    val gauge = metricRegistry.gauge(name, supplier)
    namedGauges.add(NamedGauge(name, gauge))
  }

  protected def counters(): List[NamedCounter] = {
    List.empty[NamedCounter]
  }

  def gauges(): List[NamedGauge[_]] = {
    namedGauges.toList
  }

  protected def histograms(): List[NamedHistogram] = {
    List.empty[NamedHistogram]
  }

  protected def timers(): List[NamedTimer] = {
    List.empty[NamedTimer]
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

  protected def doStartTimer(metricsName: String, key: String): Unit = {
    throw new UnsupportedOperationException()
  }

  protected def doStopTimer(metricsName: String, key: String): Unit = {
    throw new UnsupportedOperationException()
  }

  override def incCounter(metricsName: String, incV: Long = 1): Unit = {
    throw new UnsupportedOperationException("updateCounter has not been implemented!")
  }

  override def decCounter(metricsName: String, decV: Long = 1): Unit = {
    throw new UnsupportedOperationException("updateCounter has not been implemented!")
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
    sb.append(s"${normalizeKey(nc.name)}Count$countersLabel ${nc.counter.getCount} $timestamp\n")

    updateInnerMetrics(sb.toString())
  }

  def recordGauge(ng: NamedGauge[_]): Unit = {
    val timestamp = System.currentTimeMillis()
    val sb = new StringBuilder
    sb.append(s"${normalizeKey(ng.name)}Value$guagesLabel ${ng.gaurge.getValue} $timestamp\n")

    updateInnerMetrics(sb.toString())
  }

  def recordHistogram(nh: NamedHistogram): Unit = {
    val timestamp = System.currentTimeMillis()
    val sb = new StringBuilder
    val metricName = nh.name
    val h = nh.histogram
    val snapshot = h.getSnapshot
    val prefix = normalizeKey(metricName)
    sb.append(s"${prefix}Count$histogramslabels ${h.getCount} $timestamp\n")
    sb.append(s"${prefix}Max$histogramslabels ${reportNanosAsMills(snapshot.getMax)} $timestamp\n")
    sb.append(s"${prefix}Mean$histogramslabels ${reportNanosAsMills(snapshot.getMean)} $timestamp\n")
    sb.append(s"${prefix}Min$histogramslabels ${reportNanosAsMills(snapshot.getMin)} $timestamp\n")
    sb.append(s"${prefix}50thPercentile$histogramslabels ${reportNanosAsMills(snapshot.getMedian)} $timestamp\n")
    sb.append(s"${prefix}75thPercentile$histogramslabels ${reportNanosAsMills(snapshot.get75thPercentile)} $timestamp\n")
    sb.append(s"${prefix}95thPercentile$histogramslabels ${reportNanosAsMills(snapshot.get95thPercentile)} $timestamp\n")
    sb.append(s"${prefix}98thPercentile$histogramslabels ${reportNanosAsMills(snapshot.get98thPercentile)} $timestamp\n")
    sb.append(s"${prefix}99thPercentile$histogramslabels ${reportNanosAsMills(snapshot.get99thPercentile)} $timestamp\n")
    sb.append(s"${prefix}999thPercentile$histogramslabels ${reportNanosAsMills(snapshot.get999thPercentile)} $timestamp\n")

    updateInnerMetrics(sb.toString())
  }

  def recordTimer(nt: NamedTimer): Unit = {
    val timestamp = System.currentTimeMillis()
    val sb = new StringBuilder
    val snapshot = nt.timer.getSnapshot
    val prefix = normalizeKey(nt.name)
    sb.append(s"${prefix}Count$timersLabels ${nt.timer.getCount} $timestamp\n")
    sb.append(s"${prefix}Max$histogramslabels ${reportNanosAsMills(snapshot.getMax)} $timestamp\n")
    sb.append(s"${prefix}Mean$histogramslabels ${reportNanosAsMills(snapshot.getMean)} $timestamp\n")
    sb.append(s"${prefix}Min$histogramslabels ${reportNanosAsMills(snapshot.getMin)} $timestamp\n")
    sb.append(s"${prefix}50thPercentile$histogramslabels ${reportNanosAsMills(snapshot.getMedian)} $timestamp\n")
    sb.append(s"${prefix}75thPercentile$histogramslabels ${reportNanosAsMills(snapshot.get75thPercentile)} $timestamp\n")
    sb.append(s"${prefix}95thPercentile$histogramslabels ${reportNanosAsMills(snapshot.get95thPercentile)} $timestamp\n")
    sb.append(s"${prefix}98thPercentile$histogramslabels ${reportNanosAsMills(snapshot.get98thPercentile)} $timestamp\n")
    sb.append(s"${prefix}99thPercentile$histogramslabels ${reportNanosAsMills(snapshot.get99thPercentile)} $timestamp\n")
    sb.append(s"${prefix}999thPercentile$histogramslabels ${reportNanosAsMills(snapshot.get999thPercentile)} $timestamp\n")

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

  val guagesLabel = """{type="gauges"}"""
  val countersLabel = """{type="counters"}"""
  val metersLabel = countersLabel
  val histogramslabels = """{type="histograms"}"""
  val timersLabels = """{type="timers"}"""
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