package com.aliyun.emr.jss.protocol.message

import com.aliyun.emr.jss.protocol.PartitionLocation

sealed trait ShuffleMessage extends Serializable

object ShuffleMessages {

  case class RegisterShuffle(
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int) extends ShuffleMessage

  case class RegisterShuffleResponse(
      success: Boolean,
      partitionLocations: List[PartitionLocation]) extends ShuffleMessage

  // mode == 1 means master partition
  // else means slave partition
  case class RegisterShufflePartition(
      applicationId: String,
      shuffleId: Int,
      reduceId: Int,
      partitionMemoryBytes: Int,
      mode: Int, chunkIndex: String) extends ShuffleMessage

  case class RegisterShufflePartitionResponse(success: Boolean) extends ShuffleMessage


}
