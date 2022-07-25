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

import java.nio.ByteBuffer;

import com.aliyun.emr.rss.common.network.protocol.flink.message.*;
import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.emr.rss.common.network.buffer.NettyManagedBuffer;

/** An on-the-wire transmittable message. */
public abstract class Message implements Encodable {
  private ManagedBuffer body;

  protected Message() {
    this(null);
  }

  protected Message(ManagedBuffer body) {
    this.body = body;
  }

  /** Used to identify this request type. */
  public abstract Type type();

  /** An optional body for the message. */
  public ManagedBuffer body() {
    return body;
  }

  public void setBody(ByteBuf buf) {
    this.body = new NettyManagedBuffer(buf);
  }

  /** Whether the body should be copied out in frame decoder. */
  public boolean needCopyOut() {
    return false;
  }

  protected boolean equals(Message other) {
    return Objects.equal(body, other.body);
  }

  public ByteBuffer toByteBuffer() {
    // Allow room for encoded message, plus the type byte
    ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
    buf.writeByte(type().id());
    encode(buf);
    assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
    return buf.nioBuffer();
  }

  /** Preceding every serialized Message is its type, which allows us to deserialize it. */
  public enum Type implements Encodable {
    UnkownType(-1),
    ChunkFetchRequest(0),
    ChunkFetchSuccess(1),
    ChunkFetchFailure(2),
    RpcRequest(3),
    RpcResponse(4),
    RpcFailure(5),
    OpenStreamReduce(6),
    StreamHandleReduce(7),
    OneWayMessage(9),
    PushData(11),
    PushMergedData(12),

    ErrorResponse(101),
    WriteRegionStart(102),
    WriteRegionFinish(103),
    WriteFinish(104),
    WriteFinishCommit(105),
    OpenStreamMap(106),
    ReadAddCredit(107),
    ReadData(108),
    StreamHandleMap(109),
    BacklogAnnouncement(110),
    ;

    private final byte id;

    Type(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.id = (byte) id;
    }

    public byte id() { return id; }

    @Override public int encodedLength() { return 1; }

    @Override public void encode(ByteBuf buf) { buf.writeByte(id); }

    public static Type decode(ByteBuf buf) {
      byte id = buf.readByte();
      switch (id) {
        case 0: return ChunkFetchRequest;
        case 1: return ChunkFetchSuccess;
        case 2: return ChunkFetchFailure;
        case 3: return RpcRequest;
        case 4: return RpcResponse;
        case 5: return RpcFailure;
        case 6: return OpenStreamReduce;
        case 7: return StreamHandleReduce;
        case 9: return OneWayMessage;
        case 11: return PushData;
        case 12: return PushMergedData;
        case 101: return ErrorResponse;
        case 102: return WriteRegionStart;
        case 103: return WriteRegionFinish;
        case 104: return WriteFinish;
        case 105: return WriteFinishCommit;
        case 106: return OpenStreamMap;
        case 107: return ReadAddCredit;
        case 108: return ReadData;
        case 109: return StreamHandleMap;
        case 110: return BacklogAnnouncement;
        case -1: throw new IllegalArgumentException("User type messages cannot be decoded.");
        default: throw new IllegalArgumentException("Unknown message type: " + id);
      }
    }
  }

  public static Message decode(Type msgType, ByteBuf in) {
    return decode(msgType, in, true);
  }


  public static Message decode(Type msgType, ByteBuf in, boolean decodeBody) {
    switch (msgType) {
      case ChunkFetchRequest:
        return ChunkFetchRequest.decode(in);
      case ChunkFetchSuccess:
        return ChunkFetchSuccess.decode(in, decodeBody);
      case ChunkFetchFailure:
        return ChunkFetchFailure.decode(in);
      case RpcRequest:
        return RpcRequest.decode(in, decodeBody);
      case RpcResponse:
        return RpcResponse.decode(in, decodeBody);
      case RpcFailure:
        return RpcFailure.decode(in);
      case OpenStreamReduce:
        return OpenStreamReduce.decode(in);
      case StreamHandleReduce:
        return StreamHandleReduce.decode(in);
      case OneWayMessage:
        return OneWayMessage.decode(in, decodeBody);
      case PushData:
        return PushData.decode(in, decodeBody);
      case PushMergedData:
        return PushMergedData.decode(in, decodeBody);

      case ErrorResponse:
        return ErrorResponse.decode(in);
      case WriteRegionStart:
        return WriteRegionStart.decode(in);
      case WriteRegionFinish:
        return WriteRegionFinish.decode(in);
      case WriteFinish:
        return WriteFinish.decode(in);
      case WriteFinishCommit:
        return WriteFinishCommit.decode(in);
      case OpenStreamMap:
        return OpenStreamMap.decode(in);
      case ReadAddCredit:
        return ReadAddCredit.decode(in);
      case ReadData:
        return ReadData.decode(in);
      case StreamHandleMap:
        return StreamHandleMap.decode(in);
      case BacklogAnnouncement:
        return BacklogAnnouncement.decode(in);

      default:
        throw new IllegalArgumentException("Unexpected message type: " + msgType);
    }
  }

  public static Message decode(ByteBuffer buffer) {
    ByteBuf buf = Unpooled.wrappedBuffer(buffer);
    Type type = Type.decode(buf);
    return decode(type, buf);
  }
}
