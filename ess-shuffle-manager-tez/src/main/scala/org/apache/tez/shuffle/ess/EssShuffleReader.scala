package org.apache.tez.shuffle.ess

import java.util

import com.aliyun.emr.ess.client.ShuffleClient
import com.aliyun.emr.ess.client.stream.EssInputStream
import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.unsafe.Platform

import org.apache.spark.SparkConf

import scala.collection.convert.ImplicitConversions._

import com.aliyun.emr.ess.common.internal.Logging

class EssShuffleReader[K, C](
  appId: String,
  shuffleId: Int,
  startPartition: Int,
  endPartition: Int,
  emptyShuffle: Boolean,
  taskAttemptId: Int,
  essConf: EssConf) extends Logging {

  private val essShuffleClient = ShuffleClient.get(essConf)

  private val inputStreams = new util.ArrayList[EssInputStream]()

  def read(): Iterator[Product4[Int, Int, K, C]] = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.kryo.classesToRegister", "org.apache.hadoop.hive.ql.io.HiveKey,org.apache.hadoop.io.BytesWritable")

    val recordIter = (startPartition until endPartition).map(reduceId => {
      if (!emptyShuffle) {
        val inputStream = essShuffleClient.readPartition(
          appId, shuffleId, reduceId, taskAttemptId)
        inputStreams.add(inputStream)
        inputStream
      } else {
        EssInputStream.empty()
      }
    }).toIterator.flatMap(
      new NextIterator(_)
    )

    recordIter.asInstanceOf[Iterator[(Int, Int, K, C)]]
  }

  def close(): Unit = {
    inputStreams.foreach(_.close())
  }

  class NextIterator(inputStream: EssInputStream) extends Iterator[(Int, Int, Any, Any)] {

    private var gotNext = false
    private var nextValue: (Int, Int, Any, Any) = _
    private var closed = false
    protected var finished = false
    val lengthBuf = new Array[Byte](8)
    var key = new Array[Byte](1024 * 1024)
    var value = new Array[Byte](1024 * 1024)

    protected def getNext(): (Int, Int, Any, Any) = {
      if (inputStream.read(lengthBuf, 0, 8) == -1) {
        finished = true;
        return null;
      }
      val keyLen = Platform.getInt(lengthBuf, Platform.BYTE_ARRAY_OFFSET)
      if (key.length < keyLen) {
        key = new Array[Byte](keyLen)
      }
      val valLen = Platform.getInt(lengthBuf, Platform.BYTE_ARRAY_OFFSET + 4)
      if (value.length < valLen) {
        value = new Array[Byte](valLen)
      }
      inputStream.read(key, 0, keyLen)
      inputStream.read(value, 0, valLen)
      (keyLen, valLen, key, value)
    }

    protected def close(): Unit = {
      inputStream.close()
    }

    def closeIfNeeded() {
      if (!closed) {
        // Note: it's important that we set closed = true before calling close(), since setting it
        // afterwards would permit us to call close() multiple times if close() threw an exception.
        closed = true
        close()
      }
    }

    override def hasNext: Boolean = {
      if (!finished) {
        if (!gotNext) {
          nextValue = getNext()
          if (finished) {
            closeIfNeeded()
          }
          gotNext = true
        }
      }
      !finished
    }

    override def next(): (Int, Int, Any, Any) = {
      if (!hasNext) {
        throw new NoSuchElementException("End of stream")
      }
      gotNext = false
      nextValue
    }
  }

}
