package com.aliyun.emr.jss.protocol.message

import java.util

import com.aliyun.emr.jss.common.rpc.RpcEndpointRef
import com.aliyun.emr.jss.protocol.PartitionLocation

sealed trait Message extends Serializable
sealed trait MasterMessage extends Message
sealed trait WorkerMessage extends Message
sealed trait ClientMessage extends Message

object ControlMessages {

  /** ==========================================
   *         handled by master
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

  case class Trigger(
    applicationId: String,
    shuffleId: Int) extends MasterMessage

  case class TriggerResponse(
    success: Boolean) extends MasterMessage

  case class StageEnd(
    applicationId: String,
    shuffleId: Int
  ) extends MasterMessage

  case class StageEndResponse(
    errorCode: ReturnCode,
    lostFiles: util.List[String]
  ) extends MasterMessage


  /** ==========================================
   *         handled by worker
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
    shuffleKey: String,
    masterLocations: util.List[PartitionLocation],
    slaveLocations: util.List[PartitionLocation]
  ) extends WorkerMessage

  case class ReserveBuffersResponse(
    success: Boolean
  ) extends WorkerMessage

  case class CommitFiles(
    shuffleKey: String,
    commitLocations: util.List[PartitionLocation],
    mode: PartitionLocation.Mode) extends WorkerMessage
  case class CommitFilesResponse(
    errorCode: ReturnCode,
    failedLocations: util.List[PartitionLocation]
  ) extends WorkerMessage

  case class Destroy(
    shuffleKey: String,
    masterLocations: util.List[PartitionLocation],
    slaveLocation: util.List[PartitionLocation]
  ) extends WorkerMessage
  case class DestroyResponse(
    returnCode: ReturnCode,
    failedMasters: util.List[PartitionLocation],
    failedSlaves: util.List[PartitionLocation]
  ) extends WorkerMessage

  /** ==========================================
   *              common
   *  ==========================================
   */
  case class SlaveLost(
    shuffleKey: String,
    masterLocation: PartitionLocation,
    slaveLocation: PartitionLocation
  ) extends Message

  case class SlaveLostResponse(
    returnCode: ReturnCode,
    slaveLocation: PartitionLocation
  ) extends Message

  // for test
  case object GetWorkerInfos extends Message
  case class GetWorkerInfosResponse(
    success: Boolean,
    workerInfos: Any
  ) extends Message
}

object DataMessages {
/** ==========================================
 *             handled by master
 *  ==========================================
 */

  /** ==========================================
   *             handled by worker
   *  ==========================================
   */
  case class SendData(
    shuffleKey: String,
    partitionLocation: PartitionLocation,
    mode: PartitionLocation.Mode,
    flush: Boolean,
//    data: ByteBuf
    data: Array[Byte]
  ) extends WorkerMessage

  case class ReplicateData(
    shuffleKey: String,
    partitionLocation: PartitionLocation,
    working: Int,
    masterData: Array[Byte],
    slaveData: Array[Byte]
  ) extends WorkerMessage

  case class ReplicateDataResponse(
    returnCode: ReturnCode,
    msg: String
  ) extends WorkerMessage

  case class GetDoubleChunkInfo(
    shuffleKey: String,
    mode: PartitionLocation.Mode,
    partitionLocation: PartitionLocation
  ) extends WorkerMessage

  case class GetDoubleChunkInfoResponse(
    success: Boolean,
    working: Int,
    masterRemaining: Int,
    masterData: Array[Byte],
    slaveRemaining: Int,
    slaveData: Array[Byte]
  ) extends WorkerMessage

  /** ==========================================
   *             handled by client
   *  ==========================================
   */
  case class SendDataResponse(
    success: Boolean,
    msg: String
  ) extends ClientMessage
}
