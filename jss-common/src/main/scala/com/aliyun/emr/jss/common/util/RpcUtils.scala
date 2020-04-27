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

package com.aliyun.emr.jss.common.util

import com.aliyun.emr.jss.common.JindoConf
import com.aliyun.emr.jss.common.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcTimeout}

private[jss] object RpcUtils {

  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: JindoConf): Int = {
    conf.getInt("jindo.rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: JindoConf): Long = {
    conf.getTimeAsMs("jindo.rpc.retry.wait", "3s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  def askRpcTimeout(conf: JindoConf): RpcTimeout = {
    RpcTimeout(conf, Seq("jindo.rpc.askTimeout", "jindo.network.timeout"), "120s")
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: JindoConf): RpcTimeout = {
    RpcTimeout(conf, Seq("jindo.rpc.lookupTimeout", "jindo.network.timeout"), "120s")
  }

  private val MAX_MESSAGE_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /** Returns the configured max message size for messages in bytes. */
  def maxMessageSizeBytes(conf: JindoConf): Int = {
    val maxSizeInMB = conf.getInt("jindo.rpc.message.maxSize", 128)
    if (maxSizeInMB > MAX_MESSAGE_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"jindo.rpc.message.maxSize should not be greater than $MAX_MESSAGE_SIZE_IN_MB MB")
    }
    maxSizeInMB * 1024 * 1024
  }
}
