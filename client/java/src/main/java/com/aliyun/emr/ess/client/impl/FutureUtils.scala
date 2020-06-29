package com.aliyun.emr.ess.client.impl

import java.util

import scala.concurrent.Future
import scala.util.{Failure, Success}

import com.aliyun.emr.ess.common.util.ThreadUtils
import com.aliyun.emr.ess.protocol.message.DataMessages.PushDataResponse
import com.aliyun.emr.ess.protocol.PartitionLocation
import com.aliyun.emr.ess.protocol.message.StatusCode

object FutureUtils {
  def futureOncomplete(future: Future[PushDataResponse],
    mapKey: String,
    mapWrittenPartitions: util.Map[String, util.HashSet[PartitionLocation]],
    loc: PartitionLocation
  ): Unit = {
    future.onComplete {
      case Success(res) =>
        if (res.status == StatusCode.Success) {
          if (!mapWrittenPartitions.containsKey(mapKey)) if (!mapWrittenPartitions.containsKey(mapKey)) {
            val locations: util.HashSet[PartitionLocation] = new util.HashSet[PartitionLocation]
            mapWrittenPartitions.put(mapKey, locations)
          }
          val locations: util.Set[PartitionLocation] = mapWrittenPartitions.get(mapKey)
          locations.add(loc)
        }
      case Failure(e) =>
    }(ThreadUtils.sameThread)
  }
}
