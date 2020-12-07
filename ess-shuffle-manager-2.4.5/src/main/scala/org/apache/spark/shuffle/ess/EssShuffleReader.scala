package org.apache.spark.shuffle.ess

import com.aliyun.emr.ess.client.stream.EssInputStream
import com.aliyun.emr.ess.client.{MetricsCallback, ShuffleClient}
import org.apache.spark.{InterruptibleIterator, SparkConf, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReader}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

class EssShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    sparkConf: SparkConf)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency
  private val essShuffleClient = ShuffleClient.get(EssShuffleManager.fromSparkConf(sparkConf))

  override def read(): Iterator[Product2[K, C]] = {

    val serializerInstance = dep.serializer.newInstance()

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesRead: Long): Unit =
        readMetrics.incRemoteBytesRead(bytesRead)

      override def incReadTime(time: Long): Unit =
        readMetrics.incFetchWaitTime(time)
    }

    val recordIter = (startPartition until endPartition).map(reduceId => {
      if (handle.numMaps > 0) {
        val start = System.currentTimeMillis()
        val inputStream = essShuffleClient.readPartition(
          handle.asInstanceOf[EssShuffleHandle[_, _]].newAppId, handle.shuffleId, reduceId, context.attemptNumber())
        metricsCallback.incReadTime(System.currentTimeMillis() - start)
        inputStream.setCallback(metricsCallback)
        // ensure inputStream is closed when task completes
        context.addTaskCompletionListener(_ => inputStream.close())
        inputStream
      } else {
        EssInputStream.empty()
      }
    }).toIterator.flatMap(
      serializerInstance.deserializeStream(_).asKeyValueIterator
    )

    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        context.addTaskCompletionListener[Unit](_ => {
          sorter.stop()
        })
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}
