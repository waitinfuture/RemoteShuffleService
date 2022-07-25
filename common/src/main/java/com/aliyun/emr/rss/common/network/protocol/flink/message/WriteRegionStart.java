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

import com.aliyun.emr.rss.common.network.protocol.Encoders;
import com.aliyun.emr.rss.common.network.protocol.RequestMessage;

public final class WriteRegionStart extends RequestMessage {
  public final String shuffleKey;
  // PartitionLocation.getUniqueId()
  public final String partUniqueId;
  public final int regionIndex;
  public final boolean isBroadcast;

  public WriteRegionStart(String shuffleKey, String partitionId,
      int regionIndex, boolean isBroadcast) {
    this.shuffleKey = shuffleKey;
    this.partUniqueId = partitionId;
    this.regionIndex = regionIndex;
    this.isBroadcast = isBroadcast;
  }

  @Override
  public Type type() { return Type.WriteRegionStart; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(shuffleKey) +
      Encoders.Strings.encodedLength(partUniqueId) + 4 + 1;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, shuffleKey);
    Encoders.Strings.encode(buf, partUniqueId);
    buf.writeInt(regionIndex);
    buf.writeBoolean(isBroadcast);
  }

  public static WriteRegionStart decode(ByteBuf buf) {
    return new WriteRegionStart(
      Encoders.Strings.decode(buf),
      Encoders.Strings.decode(buf),
      buf.readInt(),
      buf.readBoolean());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(shuffleKey, partUniqueId, regionIndex, isBroadcast);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof WriteRegionStart) {
      WriteRegionStart o = (WriteRegionStart) other;
      return shuffleKey.equals(o.shuffleKey) &&
        partUniqueId.equals(o.partUniqueId) &&
        regionIndex == o.regionIndex && isBroadcast == o.isBroadcast;
    }
    return false;
  }

  @Override
   public String toString() {
    return Objects.toStringHelper(this)
      .add("shuffleKey", shuffleKey)
      .add("partitionId", partUniqueId)
      .add("regionIndex", regionIndex)
      .add("isBroadcast", isBroadcast)
      .toString();
  }
}
