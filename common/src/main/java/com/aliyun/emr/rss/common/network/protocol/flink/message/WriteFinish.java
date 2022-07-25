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

public final class WriteFinish extends RequestMessage {
  public final String shuffleKey;
  // PartitionLocation.getUniqueId()
  public final String partUniqueId;

  public WriteFinish(String shuffleKey, String partitionId) {
    this.shuffleKey = shuffleKey;
    this.partUniqueId = partitionId;
  }

  @Override
  public Type type() { return Type.WriteFinish; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(shuffleKey) + Encoders.Strings.encodedLength(partUniqueId);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, shuffleKey);
    Encoders.Strings.encode(buf, partUniqueId);
  }

  public static WriteFinish decode(ByteBuf buf) {
    return new WriteFinish(Encoders.Strings.decode(buf), Encoders.Strings.decode(buf));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(shuffleKey, partUniqueId);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof WriteFinish) {
      WriteFinish o = (WriteFinish) other;
      return shuffleKey.equals(o.shuffleKey) &&
        partUniqueId.equals(o.partUniqueId);
    }
    return false;
  }

  @Override
   public String toString() {
    return Objects.toStringHelper(this)
      .add("shuffleKey", shuffleKey)
      .add("partitionId", partUniqueId)
      .toString();
  }
}
