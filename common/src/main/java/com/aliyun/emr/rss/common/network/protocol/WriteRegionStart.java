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

package com.aliyun.emr.rss.common.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/** Response to {@link RpcRequest} for a failed RPC. */
public final class WriteRegionStart extends AbstractMessage implements RequestMessage {
  public final long requestId;
  public final long streamId;
  public final int regionIndex;
  public final boolean isBroadcast;

  public WriteRegionStart(long requestId, long streamId, int regionIndex, boolean isBroadcast) {
    this.requestId = requestId;
    this.streamId = streamId;
    this.regionIndex = regionIndex;
    this.isBroadcast = isBroadcast;
  }

  @Override
  public Type type() { return Type.ErrorResponse; }

  @Override
  public int encodedLength() {
    return 8 + 8 + 4 + 1;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    buf.writeLong(streamId);
    buf.writeInt(regionIndex);
    buf.writeBoolean(isBroadcast);
  }

  public static WriteRegionStart decode(ByteBuf buf) {
    return new WriteRegionStart(
      buf.readLong(),
      buf.readLong(),
      buf.readInt(),
      buf.readBoolean());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, streamId, regionIndex, isBroadcast);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof WriteRegionStart) {
      WriteRegionStart o = (WriteRegionStart) other;
      return requestId == o.requestId && streamId == o.streamId &&
        regionIndex == o.regionIndex && isBroadcast == o.isBroadcast;
    }
    return false;
  }

  @Override
   public String toString() {
    return Objects.toStringHelper(this)
      .add("requestId", requestId)
      .add("streamId", streamId)
      .add("regionIndex", regionIndex)
      .add("isBroadcast", isBroadcast)
      .toString();
  }
}
