package com.aliyun.emr.ess.service.deploy.worker;

import java.nio.ByteBuffer;

import com.aliyun.emr.network.protocol.PushMergedData;
import com.aliyun.emr.network.protocol.PushData;
import com.aliyun.emr.network.client.RpcResponseCallback;
import com.aliyun.emr.network.client.TransportClient;
import com.aliyun.emr.network.server.OneForOneStreamManager;
import com.aliyun.emr.network.server.RpcHandler;
import com.aliyun.emr.network.server.StreamManager;
import com.aliyun.emr.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PushDataRpcHandler extends RpcHandler {

    private static final Logger logger = LoggerFactory.getLogger(PushDataRpcHandler.class);

    private final TransportConf conf;
    private final PushDataHandler handler;
    private final OneForOneStreamManager streamManager;

    public PushDataRpcHandler(TransportConf conf, PushDataHandler handler) {
        this.conf = conf;
        this.handler = handler;
        streamManager = new OneForOneStreamManager();
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        throw new UnsupportedOperationException("PushDataRpcHandler");
    }

    @Override
    public void receivePushData(
            TransportClient client, PushData pushData, RpcResponseCallback callback) {
        handler.handlePushData(pushData, callback);
    }

    @Override
    public void receivePushMergedData(
        TransportClient client, PushMergedData pushMergedData, RpcResponseCallback callback) {
        handler.handlePushMergedData(pushMergedData, callback);
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
