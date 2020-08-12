package com.aliyun.emr.ess.service.deploy.worker;

import java.io.File;
import java.util.ArrayList;

public interface OpenStreamHandler {
    class FileInfo {
        final File file;
        final ArrayList<Long> chunkOffsets;
        final long fileLength;

        FileInfo(File file, ArrayList<Long> chunkOffsets, long fileLength) {
            this.file = file;
            this.chunkOffsets = chunkOffsets;
            this.fileLength = fileLength;
        }
    }

    FileInfo handleOpenStream(String shuffleKey, String partitionId);
}
