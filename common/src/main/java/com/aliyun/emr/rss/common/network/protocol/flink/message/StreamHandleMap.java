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

package com.aliyun.emr.rss.common.network.protocol.flink.message;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import com.aliyun.emr.rss.common.network.protocol.RequestMessage;

public final class StreamHandleMap extends RequestMessage {
  public final long streamId;

  public StreamHandleMap(long streamId) {
    this.streamId = streamId;
  }

  @Override
  public Type type() { return Type.StreamHandleReduce; }

  @Override
  public int encodedLength() {
    return 8;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(streamId);
  }

  public static StreamHandleMap decode(ByteBuf buf) {
    return new StreamHandleMap(buf.readLong());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamHandleMap) {
      StreamHandleMap o = (StreamHandleMap) other;
      return streamId == o.streamId;
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .toString();
  }
}
