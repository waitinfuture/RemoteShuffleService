package com.aliyun.emr.network.server;

import java.io.File;
import java.util.ArrayList;

public class FileInfo {
  public final File file;
  public final ArrayList<Long> chunkOffsets;
  public final long fileLength;
  public final int numChunks;

  public FileInfo(File file, ArrayList<Long> chunkOffsets, long fileLength) {
    this.file = file;
    this.chunkOffsets = chunkOffsets;
    this.fileLength = fileLength;
    this.numChunks = chunkOffsets.size() - 1;
  }
}