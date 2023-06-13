/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.client

import java.util

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.ControlMessages.{ChangeLocationResponse, ChangeLocationsResponse, RegisterShuffleResponse}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcCallContext

trait RequestLocationCallContext {
  def reply(
      partitionId: Int,
      status: StatusCode,
      partitionLocationOpt: Option[PartitionLocation]): Unit
}

case class ChangeLocationCallContext(context: RpcCallContext) extends RequestLocationCallContext {
  override def reply(
      partitionId: Int,
      status: StatusCode,
      partitionLocationOpt: Option[PartitionLocation]): Unit = {
    context.reply(ChangeLocationResponse(status, partitionLocationOpt))
  }
}

case class ChangeLocationsCallContext(
    shuffleId: Int,
    context: RpcCallContext,
    mapIds: util.List[Integer],
    attemptIds: util.List[Integer],
    partitionIds: util.List[Integer])
  extends RequestLocationCallContext with Logging {
  val statuses = new Array[StatusCode](mapIds.size())
  val newLocs = new Array[PartitionLocation](mapIds.size())
  0 until statuses.length foreach (idx => statuses(idx) = null)
  @volatile var count = 0

  def markMapperEnd(mapId: Int): Unit = this.synchronized {
    0 until mapIds.size() foreach (idx => {
      if (mapIds.get(idx) == mapId) {
        statuses(idx) = StatusCode.MAP_ENDED
        newLocs(idx) = new PartitionLocation()
        count += 1
      }
    })
    if (count == mapIds.size()) {
      doReply();
    }
  }

  override def reply(
      partitionId: Int,
      status: StatusCode,
      partitionLocationOpt: Option[PartitionLocation]): Unit = this.synchronized {
    0 until mapIds.size() foreach (idx => {
      if (partitionIds.get(idx) == partitionId) {
        if (statuses(idx) != null) {
//          logInfo("this partition has already been replied!")
        } else {
          statuses(idx) = status
          newLocs(idx) = partitionLocationOpt.getOrElse(new PartitionLocation())
          count += 1
        }
      }
    })
    logInfo(
      s"after reply, shuffleId ${shuffleId}, partitionId ${partitionId}, mapIds.size is ${mapIds.size()}, count is ${count}, epoch is ${partitionLocationOpt.get.getEpoch}")

    if (count == mapIds.size()) {
      doReply()
    }
  }

  private def doReply(): Unit = this.synchronized {
    context.reply(ChangeLocationsResponse(mapIds, attemptIds, partitionIds, statuses, newLocs))
//    count = 0
  }
}

case class ApplyNewLocationCallContext(context: RpcCallContext) extends RequestLocationCallContext {
  override def reply(
      partitionId: Int,
      status: StatusCode,
      partitionLocationOpt: Option[PartitionLocation]): Unit = {
    partitionLocationOpt match {
      case Some(partitionLocation) =>
        context.reply(RegisterShuffleResponse(status, Array(partitionLocation)))
      case None => context.reply(RegisterShuffleResponse(status, Array.empty))
    }
  }
}
