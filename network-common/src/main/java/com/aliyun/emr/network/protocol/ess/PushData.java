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

package com.aliyun.emr.network.protocol.ess;

import com.aliyun.emr.network.buffer.ManagedBuffer;
import com.aliyun.emr.network.buffer.NettyManagedBuffer;
import com.aliyun.emr.network.protocol.AbstractMessage;
import com.aliyun.emr.network.protocol.Encoders;
import com.aliyun.emr.network.protocol.MessageEncoder;
import com.aliyun.emr.network.protocol.RequestMessage;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

public final class PushData extends AbstractMessage implements RequestMessage {
  public final String shuffleKey;
  public final String partitionId;
  // 0 for master, 1 for slave, see PartitionLocation.Mode
  public final byte mode;
  public final long requestId;
  public final String batchId;

  public PushData(String shuffleKey, String partitionId, byte mode, ManagedBuffer buffer,
                  long requestId, String batchId) {
    super(buffer, true);
    this.shuffleKey = shuffleKey;
    this.partitionId = partitionId;
    this.mode = mode;
    this.requestId = requestId;
    this.batchId = batchId;
  }

  @Override
  public Type type() { return Type.PushData; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(shuffleKey)
        + Encoders.Strings.encodedLength(partitionId)
        + 8 + 1 + Encoders.Strings.encodedLength(batchId);
  }

  /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, shuffleKey);
    Encoders.Strings.encode(buf, partitionId);
    buf.writeByte(mode);
    buf.writeLong(requestId);
    Encoders.Strings.encode(buf, batchId);
  }

  /** Decoding uses the given ByteBuf as our data, and will retain() it. */
  public static PushData decode(ByteBuf buf) {
    String shuffleKey = Encoders.Strings.decode(buf);
    String partitionId = Encoders.Strings.decode(buf);
    byte mode = buf.readByte();
    long requestId = buf.readLong();
    String batchId = Encoders.Strings.decode(buf);
    buf.retain();
    NettyManagedBuffer managedBuf = new NettyManagedBuffer(buf.duplicate());
    return new PushData(shuffleKey, partitionId, mode, managedBuf, requestId, batchId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(shuffleKey, partitionId, body());
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof PushData) {
      PushData o = (PushData) other;
      return shuffleKey.equals(o.shuffleKey) && partitionId.equals((o.partitionId))
          && super.equals(o);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("shuffleKey", shuffleKey)
      .add("partitionId", partitionId)
      .add("buffer", body())
      .toString();
  }
}
