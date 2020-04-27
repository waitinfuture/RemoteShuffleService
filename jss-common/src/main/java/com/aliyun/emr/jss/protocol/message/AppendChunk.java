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

package com.aliyun.emr.jss.protocol.message;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;

// Needed by ScalaDoc. See SPARK-7726

/** Request to append a shuffle chunk. Returns nothing (empty byte array). */
public class AppendChunk extends BaseChunkMessage {
  public final String appId;
  public final String chunkId;
  public final byte[] data;

  public AppendChunk(
      String appId,
      String chunkId,
      byte[] data) {
    this.appId = appId;
    this.chunkId = chunkId;
    this.data = data;
  }

  @Override
  protected Type type() {
    return Type.APPEND_CHUNK;
  }

  @Override
  public int hashCode() {
    int objectsHashCode = Objects.hashCode(appId, chunkId);
    return objectsHashCode * 41 + Arrays.hashCode(data);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("appId", appId)
      .add("chunkId", chunkId)
      .add("data size", data.length)
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof AppendChunk) {
      AppendChunk o = (AppendChunk) other;
      return Objects.equal(appId, o.appId)
        && Objects.equal(chunkId, o.chunkId)
        && Arrays.equals(data, o.data);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.Strings.encodedLength(chunkId)
      + Encoders.ByteArrays.encodedLength(data);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.Strings.encode(buf, chunkId);
    Encoders.ByteArrays.encode(buf, data);
  }

  public String id() {
    return appId + "_" + chunkId;
  }

  public static AppendChunk decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String chunkId = Encoders.Strings.decode(buf);
    byte[] data = Encoders.ByteArrays.decode(buf);
    return new AppendChunk(appId, chunkId, data);
  }
}
