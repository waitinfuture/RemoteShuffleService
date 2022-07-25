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

public final class OpenStreamMap extends RequestMessage {
  public final String shuffleKey;
  public final String fileName;
  public final int startReduceIndex;
  public final int endReduceIndex;
  public final int initialCredit;
  public final int bufferSize;

  public OpenStreamMap(
      String shuffleKey,
      String fileName,
      int startReduceIndex,
      int endReduceIndex,
      int initialCredit,
      int bufferSize) {
    this.shuffleKey = shuffleKey;
    this.fileName = fileName;
    this.startReduceIndex = startReduceIndex;
    this.endReduceIndex = endReduceIndex;
    this.initialCredit = initialCredit;
    this.bufferSize = bufferSize;
  }

  @Override
  public Type type() { return Type.OpenStreamMap; }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(shuffleKey) +
      Encoders.Strings.encodedLength(fileName) + 4 + 4 + 4 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, shuffleKey);
    Encoders.Strings.encode(buf, fileName);
    buf.writeInt(startReduceIndex);
    buf.writeInt(endReduceIndex);
    buf.writeInt(initialCredit);
    buf.writeInt(bufferSize);
  }

  public static OpenStreamMap decode(ByteBuf buf) {
    return new OpenStreamMap(
      Encoders.Strings.decode(buf),
      Encoders.Strings.decode(buf),
      buf.readInt(),
      buf.readInt(),
      buf.readInt(),
      buf.readInt());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(shuffleKey, fileName,
      startReduceIndex, endReduceIndex, initialCredit, bufferSize);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OpenStreamMap) {
      OpenStreamMap o = (OpenStreamMap) other;
      return shuffleKey.equals(o.shuffleKey) &&
        fileName.equals(o.fileName) &&
        startReduceIndex == o.startReduceIndex &&
        endReduceIndex == o.endReduceIndex &&
        initialCredit == o.initialCredit &&
        bufferSize == o.bufferSize;
    }
    return false;
  }

  @Override
   public String toString() {
    return Objects.toStringHelper(this)
      .add("shuffleKey", shuffleKey)
      .add("fileName", fileName)
      .add("startReduceIndex", startReduceIndex)
      .add("endReduceIndex", endReduceIndex)
      .add("initialCredit", initialCredit)
      .add("bufferSize", bufferSize)
      .toString();
  }
}
