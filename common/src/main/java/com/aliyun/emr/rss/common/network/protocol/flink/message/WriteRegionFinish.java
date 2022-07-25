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

import java.nio.charset.StandardCharsets;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import com.aliyun.emr.rss.common.network.protocol.RequestMessage;

public final class WriteRegionFinish extends RequestMessage {
  public final byte[] shuffleKey;
  public final byte[] partUniqueId;

  public WriteRegionFinish(String shuffleKey, String partitionUniqueId) {
    this(shuffleKey.getBytes(StandardCharsets.UTF_8),
      partitionUniqueId.getBytes(StandardCharsets.UTF_8));
  }

  public WriteRegionFinish(byte[] shuffleKey, byte[] partitionUniqueId) {
    this.shuffleKey = shuffleKey;
    this.partUniqueId = partitionUniqueId;
  }

  @Override
  public Type type() { return Type.WriteRegionFinish; }

  @Override
  public int encodedLength() {
    return 4 + shuffleKey.length + 4 + partUniqueId.length;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeInt(shuffleKey.length);
    buf.writeBytes(shuffleKey);
    buf.writeInt(partUniqueId.length);
    buf.writeBytes(partUniqueId);
  }

  public static WriteRegionFinish decode(ByteBuf buf) {
    int keySize = buf.readInt();
    byte[] key = new byte[keySize];
    buf.readBytes(key);
    int idSize = buf.readInt();
    byte[] id = new byte[idSize];
    buf.readBytes(id);
    return new WriteRegionFinish(key, id);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(shuffleKey, partUniqueId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof WriteRegionFinish) {
      WriteRegionFinish o = (WriteRegionFinish) other;
      return shuffleKey.equals(o.shuffleKey) &&
        partUniqueId.equals(o.partUniqueId);
    }
    return false;
  }

  @Override
   public String toString() {
    return Objects.toStringHelper(this)
      .add("shuffleKey", new String(shuffleKey, StandardCharsets.UTF_8))
      .add("partitionUniqueId", new String(partUniqueId, StandardCharsets.UTF_8))
      .toString();
  }
}
