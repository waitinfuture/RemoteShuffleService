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

package com.aliyun.emr.ess.common.util

import com.aliyun.emr.ess.common.EssConf
import com.aliyun.emr.ess.common.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcTimeout}

private[ess] object RpcUtils {

  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: EssConf): Int = {
    conf.getInt("ess.rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: EssConf): Long = {
    conf.getTimeAsMs("ess.rpc.retry.wait", "3s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  def askRpcTimeout(conf: EssConf): RpcTimeout = {
    RpcTimeout(conf, Seq("ess.rpc.askTimeout", "ess.network.timeout"), "120s")
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: EssConf): RpcTimeout = {
    RpcTimeout(conf, Seq("ess.rpc.lookupTimeout", "ess.network.timeout"), "120s")
  }

  private val MAX_MESSAGE_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /** Returns the configured max message size for messages in bytes. */
  def maxMessageSizeBytes(conf: EssConf): Int = {
    val maxSizeInMB = conf.getInt("ess.rpc.message.maxSize", 128)
    if (maxSizeInMB > MAX_MESSAGE_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"ess.rpc.message.maxSize should not be greater than $MAX_MESSAGE_SIZE_IN_MB MB")
    }
    maxSizeInMB * 1024 * 1024
  }
}
