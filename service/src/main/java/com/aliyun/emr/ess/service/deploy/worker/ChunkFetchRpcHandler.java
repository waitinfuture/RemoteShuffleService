package com.aliyun.emr.ess.service.deploy.worker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.ess.common.exception.EssException;
import com.aliyun.emr.network.client.RpcResponseCallback;
import com.aliyun.emr.network.client.TransportClient;
import com.aliyun.emr.network.server.OneForOneStreamManager;
import com.aliyun.emr.network.server.FileInfo;
import com.aliyun.emr.network.server.ManagedBufferIterator;
import com.aliyun.emr.network.server.RpcHandler;
import com.aliyun.emr.network.server.StreamManager;
import com.aliyun.emr.network.util.TransportConf;

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
        FileInfo fileInfo = handler.handleOpenStream(shuffleKey, fileName);
        if (fileInfo != null) {
            try {
                ManagedBufferIterator iterator = new ManagedBufferIterator(fileInfo, conf);
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
}
