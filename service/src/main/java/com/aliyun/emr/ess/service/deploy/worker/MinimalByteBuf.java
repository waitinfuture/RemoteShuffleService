package com.aliyun.emr.ess.service.deploy.worker;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ByteProcessor;

public class MinimalByteBuf extends ByteBuf {

  @Override
  public int capacity() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int maxCapacity() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBufAllocator alloc() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteOrder order() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf order(ByteOrder endianness) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf unwrap() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isDirect() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadOnly() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf asReadOnly() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readerIndex() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readerIndex(int readerIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writerIndex() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writerIndex(int writerIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setIndex(int readerIndex, int writerIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readableBytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writableBytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int maxWritableBytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadable() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadable(int size) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWritable() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWritable(int size) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf markReaderIndex() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf resetReaderIndex() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf markWriterIndex() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf resetWriterIndex() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf discardReadBytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf discardSomeReadBytes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf ensureWritable(int minWritableBytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int ensureWritable(int minWritableBytes, boolean force) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getUnsignedByte(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShortLE(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getUnsignedShort(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getUnsignedShortLE(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMedium(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMediumLE(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getUnsignedMedium(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getUnsignedMediumLE(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntLE(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getUnsignedInt(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getUnsignedIntLE(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongLE(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public char getChar(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CharSequence getCharSequence(int index, int length, Charset charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBoolean(int index, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setShortLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setMediumLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setIntLE(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setLongLE(int index, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setChar(int index, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setFloat(int index, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setDouble(int index, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf setZero(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int setCharSequence(int index, CharSequence sequence, Charset charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean readBoolean() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte readByte() {
    throw new UnsupportedOperationException();
  }

  @Override
  public short readUnsignedByte() {
    throw new UnsupportedOperationException();
  }

  @Override
  public short readShort() {
    throw new UnsupportedOperationException();
  }

  @Override
  public short readShortLE() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readUnsignedShort() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readUnsignedShortLE() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readMedium() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readMediumLE() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readUnsignedMedium() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readUnsignedMediumLE() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readInt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readIntLE() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long readUnsignedInt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long readUnsignedIntLE() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long readLong() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long readLongLE() {
    throw new UnsupportedOperationException();
  }

  @Override
  public char readChar() {
    throw new UnsupportedOperationException();
  }

  @Override
  public float readFloat() {
    throw new UnsupportedOperationException();
  }

  @Override
  public double readDouble() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readBytes(int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readSlice(int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readRetainedSlice(int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readBytes(ByteBuf dst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readBytes(ByteBuf dst, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readBytes(ByteBuf dst, int dstIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readBytes(byte[] dst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readBytes(byte[] dst, int dstIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readBytes(ByteBuffer dst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readBytes(OutputStream out, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readBytes(GatheringByteChannel out, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CharSequence readCharSequence(int length, Charset charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int readBytes(FileChannel out, long position, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf skipBytes(int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBoolean(boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeByte(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeShort(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeShortLE(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeMedium(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeMediumLE(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeInt(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeIntLE(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeLong(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeLongLE(long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeChar(int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeFloat(float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeDouble(double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(byte[] src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeBytes(ByteBuffer src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeBytes(InputStream in, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeBytes(ScatteringByteChannel in, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeBytes(FileChannel in, long position, int length) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf writeZero(int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int writeCharSequence(CharSequence sequence, Charset charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int indexOf(int fromIndex, int toIndex, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int bytesBefore(byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int bytesBefore(int length, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int bytesBefore(int index, int length, byte value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int forEachByte(ByteProcessor processor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int forEachByte(int index, int length, ByteProcessor processor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int forEachByteDesc(ByteProcessor processor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int forEachByteDesc(int index, int length, ByteProcessor processor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf copy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf copy(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf slice() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf retainedSlice() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf slice(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf retainedSlice(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf duplicate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf retainedDuplicate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int nioBufferCount() {
    return 0;
  }

  @Override
  public ByteBuffer nioBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer[] nioBuffers() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasArray() {
    return false;
  }

  @Override
  public byte[] array() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int arrayOffset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasMemoryAddress() {
    return false;
  }

  @Override
  public long memoryAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString(Charset charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString(int index, int length, Charset charset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public int compareTo(ByteBuf buffer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf retain(int increment) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int refCnt() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf retain() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf touch() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf touch(Object hint) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean release() {
    return false;
  }

  @Override
  public boolean release(int decrement) {
    return false;
  }
}
