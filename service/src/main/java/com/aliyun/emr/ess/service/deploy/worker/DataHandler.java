package com.aliyun.emr.ess.service.deploy.worker;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.aliyun.emr.network.protocol.PushData;
import com.aliyun.emr.network.client.RpcResponseCallback;

public interface DataHandler {
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

    void handlePushData(PushData pushData, RpcResponseCallback callback);
    FileInfo handleOpenStream(String shuffleKey, String partitionId);
}
