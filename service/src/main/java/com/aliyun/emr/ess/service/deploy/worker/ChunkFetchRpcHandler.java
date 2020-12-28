package com.aliyun.emr.ess.service.deploy.worker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import com.aliyun.emr.ess.common.exception.EssException;
import com.aliyun.emr.network.buffer.FileSegmentManagedBuffer;
import com.aliyun.emr.network.buffer.ManagedBuffer;
import com.aliyun.emr.network.client.RpcResponseCallback;
import com.aliyun.emr.network.client.TransportClient;
import com.aliyun.emr.network.server.OneForOneStreamManager;
import com.aliyun.emr.network.server.RpcHandler;
import com.aliyun.emr.network.server.StreamManager;
import com.aliyun.emr.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ChunkFetchRpcHandler extends RpcHandler {

    private static final Logger logger = LoggerFactory.getLogger(ChunkFetchRpcHandler.class);

    private final TransportConf conf;
    private final OpenStreamHandler handler;
    private final OneForOneStreamManager streamManager;

    public ChunkFetchRpcHandler(TransportConf conf, OpenStreamHandler handler) {
        this.conf = conf;
        this.handler = handler;
        streamManager = new OneForOneStreamManager();
    }

    private String readString(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        String shuffleKey = readString(message);
        String fileName = readString(message);
        OpenStreamHandler.FileInfo fileInfo = handler.handleOpenStream(shuffleKey, fileName);
        if (fileInfo != null) {
            try {
                ManagedBufferIterator iterator = new ManagedBufferIterator(fileInfo);
                long streamId = streamManager.registerStream(
                        client.getClientId(), iterator, client.getChannel());

                ByteBuffer response = ByteBuffer.allocate(8 + 4);
                response.putLong(streamId);
                response.putInt(fileInfo.numChunks);
                if (fileInfo.numChunks == 0) {
                    logger.debug("Chunk size is 0! fileName " + fileName);
                }
                response.flip();
                callback.onSuccess(response);
            } catch (IOException e) {
                callback.onFailure(
                        new EssException("Chunk offsets meta exception ", e));
            }
        } else {
            callback.onFailure(new FileNotFoundException());
        }
    }

    @Override
    public boolean checkRegistered() {
        return ((Worker) handler).Registered();
    }

    @Override
    public void channelInactive(TransportClient client) {
        logger.debug("channel Inactive " + client.getSocketAddress());
    }

    @Override
    public void exceptionCaught(Throwable cause, TransportClient client) {
        logger.debug("exception caught " + cause + " " + client.getSocketAddress());
    }

    @Override
    public StreamManager getStreamManager() {
        return streamManager;
    }

    private final class ManagedBufferIterator implements Iterator<ManagedBuffer> {
        private final File file;
        private final long[] offsets;
        private final int numChunks;

        private int index;

        ManagedBufferIterator(OpenStreamHandler.FileInfo fileInfo) throws IOException {
            file = fileInfo.file;
            numChunks = fileInfo.numChunks;
            offsets = new long[numChunks + 1];
            for (int i = 0; i <= numChunks ; i++) {
                offsets[i] =  fileInfo.chunkOffsets.get(i);
            }
            if (offsets[numChunks] != fileInfo.fileLength) {
                throw new IOException(String.format("The last chunk offset %d should be equals to file length %d!",
                        offsets[numChunks], fileInfo.fileLength));
            }
        }

        @Override
        public boolean hasNext() {
            return index < numChunks;
        }

        @Override
        public ManagedBuffer next() {
            final long offset = offsets[index];
            final long length = offsets[index + 1] - offset;
            index++;
            return new FileSegmentManagedBuffer(conf, file, offset, length);
        }
    }
}
