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

import java.nio.charset.StandardCharsets;

/** Response to {@link RpcRequest} for a failed RPC. */
public final class ErrorResponse extends AbstractMessage implements ResponseMessage {
  public final long requestId;
  public final long streamId;
  public final byte[] errorMsg;

  public ErrorResponse(long requestId, long streamId, byte[] errorMsg, byte[] extraInfo) {
    this.requestId = requestId;
    this.streamId = streamId;
    this.errorMsg = errorMsg;
  }

  @Override
  public Type type() { return Type.ErrorResponse; }

  @Override
  public int encodedLength() {
    return 8 + 8 + 4 + errorMsg.length;
  }

  @Override
  public void encode(ByteBuf buf) {
    buf.writeLong(requestId);
    buf.writeLong(streamId);
    buf.writeInt(errorMsg.length);
    buf.writeBytes(errorMsg);
  }

  public static ErrorResponse decode(ByteBuf buf) {
    long requestId = buf.readLong();
    long streamId = buf.readLong();
    int errMsgBytes = buf.readInt();
    byte[] errMsg = new byte[errMsgBytes];
    buf.readBytes(errMsg);
    int extraInfoBytes = buf.readInt();
    byte[] extraInfo = new byte[extraInfoBytes];
    buf.readBytes(extraInfo);
    return new ErrorResponse(requestId, streamId, errMsg, extraInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(requestId, streamId, errorMsg);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ErrorResponse) {
      ErrorResponse o = (ErrorResponse) other;
      return requestId == o.requestId && streamId == o.streamId &&
        errorMsg.equals(o.errorMsg);
    }
    return false;
  }

  @Override
   public String toString() {
    return Objects.toStringHelper(this)
      .add("requestId", requestId)
      .add("streamId", streamId)
      .add("errorMsg", new String(errorMsg, StandardCharsets.UTF_8))
      .toString();
  }
}
