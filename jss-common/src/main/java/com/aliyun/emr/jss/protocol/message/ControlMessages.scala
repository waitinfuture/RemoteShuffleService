package com.aliyun.emr.jss.protocol.message

import com.aliyun.emr.jss.protocol.PartitionLocation
import java.util

import com.aliyun.emr.jss.common.rpc.RpcEndpointRef

sealed trait Message extends Serializable
sealed trait MasterMessage extends Message
sealed trait WorkerMessage extends Message

object ControlMessages {

  /** ==========================================
   *         handled by master messages
   *  ==========================================
   */
  case object CheckForWorkerTimeOut

  case class RegisterWorker(
    host: String,
    port: Int,
    memory: Long,
    worker: RpcEndpointRef) extends MasterMessage

  case class ReregisterWorker(
    host: String,
    port: Int,
    memory: Long,
    worker: RpcEndpointRef) extends MasterMessage

  case class Heartbeat(host: String, port: Int) extends MasterMessage

  case class RegisterShuffle(
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int
  ) extends MasterMessage

  case class RegisterShuffleResponse(
      success: Boolean,
      partitionLocations: util.List[PartitionLocation]
  ) extends MasterMessage

  case class Revive(
    applicationId: String,
    shuffleId: Int
  ) extends MasterMessage

  case class ReviveResponse(
    success: Boolean,
    newLocation: PartitionLocation
  ) extends MasterMessage

  case class OfferSlave(
    partitionId: String
  ) extends MasterMessage

  case class OfferSlaveResponse(
    success: Boolean,
    location: PartitionLocation
  ) extends MasterMessage

  case class MapperEnd(
    applicationId: String,
    shuffleId: Int,
    mapId: Int,
    attempId: Int,
    partitionIds: util.List[String]
  ) extends MasterMessage

  case class MapperEndResponse(
    success: Boolean
  ) extends MasterMessage

  case class WorkerLost(
    host: String,
    port: Int
  ) extends MasterMessage

  case class WorkerLostResponse(
    success: Boolean
  ) extends MasterMessage


  /** ==========================================
   *         handled by worker messages
   *  ==========================================
   */
  case class RegisterWorkerResponse(
    success: Boolean,
    message: String) extends WorkerMessage

  case class ReregisterWorkerResponse(
    success: Boolean
  ) extends WorkerMessage

  case object SendHeartbeat extends WorkerMessage

  case class ReserveBuffers(
    masterLocations: util.List[PartitionLocation],
    slaveLocations: util.List[String]
  ) extends WorkerMessage

  case class ReserveBuffersResponse(
    success: Boolean
  ) extends WorkerMessage

  case class ClearBuffers(
    masterLocationIds: util.List[String],
    slaveLocationIds: util.List[String]
  ) extends WorkerMessage

  case class ClearBuffersResponse(
    success: Boolean
  ) extends WorkerMessage

  /** ==========================================
   *              common messages
   *  ==========================================
   */
  case class SlaveLost(
    partitionLocation: PartitionLocation
  ) extends Message

  case class SlaveLostResponse(
    success: Boolean
  ) extends Message

}
