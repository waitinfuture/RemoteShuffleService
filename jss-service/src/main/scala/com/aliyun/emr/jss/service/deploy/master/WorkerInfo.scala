package com.aliyun.emr.jss.service.deploy.master

import com.aliyun.emr.jss.common.rpc.RpcEndpointRef
import com.aliyun.emr.jss.common.util.Utils

private[jss] class WorkerInfo(
    val id: String,
    val host: String,
    val port: Int,
    val memory: Int,
    val endpoint: RpcEndpointRef) extends Serializable {

  Utils.checkHost(host)
  assert(port > 0)

  @transient var state: WorkerState.Value = _
  @transient var memoryUsed: Int = _
  @transient var lastHeartbeat: Long = _

  init()

  def memoryFree: Int = memory - memoryUsed

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    state = WorkerState.ALIVE
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  def setMemoryUsed(memoryUsed: Int): Unit = {
    this.memoryUsed = memoryUsed
  }

  def setState(state: WorkerState.Value): Unit = {
    this.state = state
  }

  def isAlive(): Boolean = this.state == WorkerState.ALIVE
}
