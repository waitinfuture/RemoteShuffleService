package com.aliyun.emr.jss.protocol.message

import com.aliyun.emr.jss.protocol.PartitionLocation
import java.util

sealed trait Message extends Serializable

object ShuffleMessages {

  case class Register(
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int) extends Message

  case class RegisterResponse(
      success: Boolean,
      partitionLocations: util.List[PartitionLocation]) extends Message

  // mode == 1 means master partition
  // else means slave partition
  case class RegisterPartition(
      applicationId: String,
      shuffleId: Int,
      reduceId: Int,
      partitionMemoryBytes: Int,
      mode: Int, chunkIndex: String) extends Message

  case class RegisterPartitionResponse(success: Boolean) extends Message

  case class ReserveBuffers(
    masterLocations: util.List[PartitionLocation],
    slaveLocations: util.List[PartitionLocation]
  ) extends Message

  case class ReserveBuffersResponse(
    success: Boolean
  ) extends Message

  case class ClearBuffers(
    masterLocations: util.List[PartitionLocation],
    slaveLocations: util.List[PartitionLocation]
  ) extends Message

  case class ClearBuffersResponse(
    success: Boolean
  ) extends Message
}
