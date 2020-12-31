package com.aliyun.emr.network.server;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;

import com.aliyun.emr.network.buffer.FileSegmentManagedBuffer;
import com.aliyun.emr.network.buffer.ManagedBuffer;
import com.aliyun.emr.network.util.TransportConf;

public final class ManagedBufferIterator implements Iterator<ManagedBuffer> {
  private final File file;
  private final long[] offsets;
  private final int numChunks;

  private final BitSet chunkTracker;
  private final TransportConf conf;

  private int index = 0;

  public ManagedBufferIterator(FileInfo fileInfo, TransportConf conf) throws IOException {
    file = fileInfo.file;
    numChunks = fileInfo.numChunks;
    offsets = new long[numChunks + 1];
    for (int i = 0; i <= numChunks; i++) {
      offsets[i] = fileInfo.chunkOffsets.get(i);
    }
    if (offsets[numChunks] != fileInfo.fileLength) {
      throw new IOException(String.format("The last chunk offset %d should be equals to file" +
        " length %d!", offsets[numChunks], fileInfo.fileLength));
    }
    chunkTracker = new BitSet(numChunks);
    chunkTracker.clear();
    this.conf = conf;
  }

  @Override
  public boolean hasNext() {
    synchronized (chunkTracker) {
      return chunkTracker.cardinality() < numChunks;
    }
  }

  public boolean hasAlreadyRead(int chunkIndex) {
    synchronized (chunkTracker) {
      return chunkTracker.get(chunkIndex);
    }
  }

  @Override
  public ManagedBuffer next() {
    // This method is only used to clear the Managed Buffer when streamManager.connectionTerminated
    // is called.
    synchronized (chunkTracker) {
      index = chunkTracker.nextClearBit(index);
    }
    assert index < numChunks;
    return chunk(index);
  }

  public ManagedBuffer chunk(int chunkIndex) {
    synchronized (chunkTracker) {
      chunkTracker.set(chunkIndex, true);
    }
    final long offset = offsets[chunkIndex];
    final long length = offsets[chunkIndex + 1] - offset;
    return new FileSegmentManagedBuffer(conf, file, offset, length);
  }
}