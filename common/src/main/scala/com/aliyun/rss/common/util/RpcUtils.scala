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

package com.aliyun.rss.common.util

import com.aliyun.rss.common.RssConf
import com.aliyun.rss.common.rpc.RpcTimeout

private[rss] object RpcUtils {

  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: RssConf): Int = {
    conf.getInt("rss.rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: RssConf): Long = {
    conf.getTimeAsMs("rss.rpc.retry.wait", "3s")
  }

  def haClientAskRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, Seq("rss.haclient.rpc.askTimeout",
      "rss.network.timeout"), "30s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  def askRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, Seq("rss.rpc.askTimeout",
      "rss.network.timeout"), "240s")
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: RssConf): RpcTimeout = {
    RpcTimeout(conf, Seq("rss.rpc.lookupTimeout",
      "rss.network.timeout"), "30s")
  }

  private val MAX_MESSAGE_SIZE_IN_MB = Int.MaxValue / 1024 / 1024

  /** Returns the configured max message size for messages in bytes. */
  def maxMessageSizeBytes(conf: RssConf): Int = {
    val maxSizeInMB = conf.getInt("rss.rpc.message.maxSize", 128)
    if (maxSizeInMB > MAX_MESSAGE_SIZE_IN_MB) {
      throw new IllegalArgumentException(
        s"rss.rpc.message.maxSize should not be greater than $MAX_MESSAGE_SIZE_IN_MB MB")
    }
    maxSizeInMB * 1024 * 1024
  }

}
