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

import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.emr.rss.common.network.buffer.NettyManagedBuffer;
import com.aliyun.emr.rss.common.network.protocol.RequestMessage;

public final class ReadData extends RequestMessage {
  public final long streamId;
  public final int backlog;
  public final int bufferSize;

  public ReadData(
      long streamId,
      int backlog,
      int bufferSize,
      ManagedBuffer body) {
    super(body);
    this.streamId = streamId;
    this.backlog = backlog;
    this.bufferSize = bufferSize;
  }

  @Override
  public Type type() { return Type.ReadData; }

  @Override
  public int encodedLength() {
    return 8 + 4 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(streamId);
    buf.writeInt(backlog);
    buf.writeInt(bufferSize);
  }

  public static ReadData decode(ByteBuf buf) {
    return decode(buf, true);
  }

  public static ReadData decode(ByteBuf buf, boolean decodeBody) {
    NettyManagedBuffer body;
    if (decodeBody) {
      body = new NettyManagedBuffer(buf);
    } else {
      body = NettyManagedBuffer.EmptyBuffer;
    }
    return new ReadData(buf.readLong(), buf.readInt(), buf.readInt(), body);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamId, backlog, bufferSize);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ReadData) {
      ReadData o = (ReadData) other;
      return streamId == o.streamId &&
        backlog == o.backlog &&
        bufferSize == o.bufferSize &&
        super.equals(o);
    }
    return false;
  }

  @Override
   public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("backlog", backlog)
      .add("bufferSize", bufferSize)
      .add("bodySize", body().size())
      .toString();
  }
}
