package com.aliyun.emr.jss.protocol.message

import com.aliyun.emr.jss.protocol.PartitionLocation
import java.util

sealed trait Message extends Serializable

object ShuffleMessages {

  case class RegisterShuffle(
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int) extends Message

  case class RegisterShuffleResponse(
      success: Boolean,
      partitionLocations: util.List[PartitionLocation]) extends Message

  case class ReserveBuffers(
    masterLocations: util.List[PartitionLocation],
    slaveLocations: util.List[PartitionLocation]
  ) extends Message

  case class ReserveBuffersResponse(
    success: Boolean
  ) extends Message

  case class ClearBuffers(
    masterLocations: util.List[String],
    slaveLocations: util.List[String]
  ) extends Message

  case class ClearBuffersResponse(
    success: Boolean
  ) extends Message

  case class Revive(
    applicationId: String,
    shuffleId: Int,
    oldLocation: PartitionLocation
  ) extends Message

  case class ReviveResponse(
    success: Boolean,
    newLocation: PartitionLocation
  ) extends Message
}
