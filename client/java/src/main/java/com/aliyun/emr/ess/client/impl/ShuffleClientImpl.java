package com.aliyun.emr.ess.client.impl;

import com.aliyun.emr.ess.client.ShuffleClient;
import com.aliyun.emr.ess.client.compress.EssLz4Compressor;
import com.aliyun.emr.ess.client.stream.EssInputStream;
import com.aliyun.emr.ess.common.EssConf;
import com.aliyun.emr.ess.common.rpc.RpcAddress;
import com.aliyun.emr.ess.common.rpc.RpcEndpointRef;
import com.aliyun.emr.ess.common.rpc.RpcEnv;
import com.aliyun.emr.ess.common.util.Utils;
import com.aliyun.emr.ess.protocol.PartitionLocation;
import com.aliyun.emr.ess.protocol.RpcNameConstants;
import com.aliyun.emr.ess.protocol.message.ControlMessages.*;
import com.aliyun.emr.ess.protocol.message.StatusCode;
import com.aliyun.emr.ess.unsafe.Platform;
import com.aliyun.emr.network.TransportContext;
import com.aliyun.emr.network.buffer.NettyManagedBuffer;
import com.aliyun.emr.network.client.RpcResponseCallback;
import com.aliyun.emr.network.client.TransportClient;
import com.aliyun.emr.network.client.TransportClientBootstrap;
import com.aliyun.emr.network.client.TransportClientFactory;
import com.aliyun.emr.network.protocol.PushData;
import com.aliyun.emr.network.server.NoOpRpcHandler;
import com.aliyun.emr.network.util.TransportConf;
import com.google.common.collect.Lists;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.ConcurrentSet;
import org.apache.log4j.Logger;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ShuffleClientImpl extends ShuffleClient {
    private static final Logger logger = Logger.getLogger(ShuffleClientImpl.class);

    private static final byte MASTER_MODE = PartitionLocation.Mode.Master.mode();

    private final EssConf conf;
    private final int maxInFlight;

    private RpcEnv rpcEnv;
    private RpcEndpointRef master;

    private TransportClientFactory dataClientFactory;

    // key: shuffleId, value: (reduceId, PartitionLocation)
    private final Map<Integer, ConcurrentHashMap<Integer, PartitionLocation>> reducePartitionMap =
            new ConcurrentHashMap<>();

    private static class PushState {
        final AtomicInteger batchId = new AtomicInteger();
        final ConcurrentSet<Integer> inFlightBatches = new ConcurrentSet<>();
        AtomicReference<IOException> exception = new AtomicReference<>();
    }
    // key: shuffleId-mapId-attemptId
    private final Map<String, PushState> pushStates = new ConcurrentHashMap<>();
    ThreadLocal<EssLz4Compressor> lz4CompressorThreadLocal = new ThreadLocal<EssLz4Compressor>() {
        @Override
        protected EssLz4Compressor initialValue() {
            int blockSize = (int) EssConf.essPushDataBufferSize(conf);
            return new EssLz4Compressor(blockSize);
        }
    };

    private static class ReduceFileGroups {
        final PartitionLocation[][] partitionGroups;
        final int[] mapAttempts;

        ReduceFileGroups(PartitionLocation[][] partitionGroups, int[] mapAttempts) {
            this.partitionGroups = partitionGroups;
            this.mapAttempts = mapAttempts;
        }
    }
    // key: shuffleId
    private final Map<Integer, ReduceFileGroups> reduceFileGroupsMap = new ConcurrentHashMap<>();

    private volatile boolean stopped = false;

    // whether heartbeat thread has started
    private boolean heartBeatStarted = false;
    // heartbeat lock
    private final Object heartBeatLock = new Object();

    public ShuffleClientImpl() {
        this(new EssConf());
    }

    public ShuffleClientImpl(EssConf conf) {
        super();
        this.conf = conf;
        maxInFlight = EssConf.essPushDataMaxReqsInFlight(conf);
        init();
    }

    public void init() {
        // init rpc env and master endpointRef
        rpcEnv = RpcEnv.create("ShuffleClient",
            Utils.localHostName(),
            0,
            conf);
        master = rpcEnv.setupEndpointRef(
            new RpcAddress(
                EssConf.essMasterHost(conf),
                EssConf.essMasterPort(conf))
            , RpcNameConstants.MASTER_EP);

        TransportConf dataTransportConf = Utils.fromEssConf(conf, "data",
            conf.getInt("ess.data.io.threads", 0));
        TransportContext context =
            new TransportContext(dataTransportConf, new NoOpRpcHandler(), true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        dataClientFactory = context.createClientFactory(bootstraps);
    }

    private ConcurrentHashMap<Integer, PartitionLocation> registerShuffle(
        String applicationId, int shuffleId, int numMappers, int numPartitions) {
        ConcurrentHashMap<Integer, PartitionLocation> result = new ConcurrentHashMap<>();

        int numRetries = 3;
        while (numRetries > 0) {
            try {
                RegisterShuffleResponse response = master.askSync(
                    new RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions),
                    ClassTag$.MODULE$.apply(RegisterShuffleResponse.class)
                );

                if (response.status().equals(StatusCode.Success)) {
                    for (int i = 0; i < response.partitionLocations().size(); i++) {
                        PartitionLocation partitionLoc = response.partitionLocations().get(i);
                        result.put(partitionLoc.getReduceId(), partitionLoc);
                    }
                    break;
                }
            } catch (Exception e) {
                logger.error("registerShuffle failed: " + e.getMessage());
                break;
            }

            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                break;
            }
            numRetries--;
        }

        return result;
    }

    private void limitMaxInFlight(
        String mapKey, PushState pushState, int limit) throws IOException {
        if (pushState.exception.get() != null) {
            throw pushState.exception.get();
        }

        ConcurrentSet<Integer> inFlightBatches = pushState.inFlightBatches;
        int delta = 50;
        int times = 60 * 1000 / 50;
        try {
            while (times > 0) {
                if (inFlightBatches.size() <= limit) {
                    break;
                }
                if (pushState.exception.get() != null) {
                    throw pushState.exception.get();
                }
                Thread.sleep(delta);
                times--;
            }
        } catch (InterruptedException e) {
            pushState.exception.set(new IOException(e));
        }

        if (inFlightBatches.size() > limit) {
            for (int req : inFlightBatches) {
                logger.error(mapKey + " batch " + req);
            }
            throw new IOException("wait timeout for task " + mapKey, pushState.exception.get());
        }
        if (pushState.exception.get() != null) {
            throw pushState.exception.get();
        }
    }

    private boolean revive(String applicationId, int shuffleId, PartitionLocation location) {
        ReviveResponse response = master.askSync(
            new Revive(applicationId, shuffleId, location),
            ClassTag$.MODULE$.apply(ReviveResponse.class)
        );

        // per partitionKey only serve single PartitionLocation in Client Cache.
        if (response.status().equals(StatusCode.Success)) {
            ConcurrentHashMap<Integer, PartitionLocation> map = reducePartitionMap.get(shuffleId);
            map.put(location.getReduceId(), response.partitionLocation());
            return true;
        } else {
            logger.error("revive failed, " + response.status());
            return false;
        }
    }

    @Override
    public int pushData(
        String applicationId,
        int shuffleId,
        int mapId,
        int attemptId,
        int reduceId,
        byte[] data,
        int offset,
        int length,
        int numMappers,
        int numPartitions) throws IOException {
        // register shuffle if not registered
        final ConcurrentHashMap<Integer, PartitionLocation> map =
            reducePartitionMap.computeIfAbsent(shuffleId, (id) ->
                registerShuffle(applicationId, shuffleId, numMappers, numPartitions));

        // get location
        PartitionLocation loc = map.get(reduceId);
        if (loc == null) {
            String msg = "loc is NULL! shuffleKey"
                + Utils.makeShuffleKey(applicationId, shuffleId) + ", reduceId " + reduceId;
            throw new IOException(msg);
        }

        final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
        PushState pushState = pushStates.computeIfAbsent(mapKey, (s) -> new PushState());
        limitMaxInFlight(mapKey, pushState, maxInFlight);

        // increment batchId
        final int nextBatchId = pushState.batchId.addAndGet(1);

        // compress data
        final EssLz4Compressor compressor = lz4CompressorThreadLocal.get();
        compressor.compress(data, offset, length);

        final int compressedTotalSize = compressor.getCompressedTotalSize();
        final int BATCH_HEADER_SIZE = 4 * 4;
        final byte[] body = new byte[BATCH_HEADER_SIZE + compressedTotalSize];
        Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET, mapId);
        Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 4, attemptId);
        Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 8, nextBatchId);
        Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 12, compressedTotalSize);
        System.arraycopy(compressor.getCompressedBuffer(), 0, body, BATCH_HEADER_SIZE,
            compressedTotalSize);

        // add inFlight requests
        pushState.inFlightBatches.add(nextBatchId);

        // build PushData request
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
        NettyManagedBuffer buffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
        PushData pushData = new PushData(MASTER_MODE, shuffleKey, loc.getUniqueId(), buffer);

        // build callback
        RpcResponseCallback callback = new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                pushState.inFlightBatches.remove(nextBatchId);
            }

            @Override
            public void onFailure(Throwable e) {
                pushState.exception.compareAndSet(null, new IOException(e));
                logger.error("PushData Exception", e);
            }
        };

        RpcResponseCallback wrappedCallback = new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                callback.onSuccess(response);
            }

            @Override
            public void onFailure(Throwable e) {
                // revive and push again on failure
                logger.error("PushData failed!", e);
                if (revive(applicationId, shuffleId, loc)) {
                    PartitionLocation newLoc = reducePartitionMap.get(shuffleId).get(reduceId);
                    try {
                        TransportClient client = dataClientFactory.createClient(
                            newLoc.getHost(), newLoc.getPort(), reduceId);
                        NettyManagedBuffer newBuffer =
                            new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
                        PushData newPushData =
                            new PushData(MASTER_MODE, shuffleKey, newLoc.getUniqueId(), newBuffer);
                        client.pushData(newPushData, callback);
                    } catch (Exception ex) {
                        logger.error(ex);
                        callback.onFailure(ex);
                    }
                } else {
                    logger.error("Revive failed!");
                    callback.onFailure(e);
                }
            }
        };

        // do push data
        try {
            TransportClient client =
                dataClientFactory.createClient(loc.getHost(), loc.getPort(), reduceId);
            client.pushData(pushData, wrappedCallback);
        } catch (Exception e) {
            wrappedCallback.onFailure(e);
        }

        return body.length;
    }

    @Override
    public void mapperEnd(
        String applicationId,
        int shuffleId,
        int mapId,
        int attemptId,
        int numMappers) throws IOException {
        final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
        PushState pushState = pushStates.computeIfAbsent(mapKey, (s) -> new PushState());

        try {
            limitMaxInFlight(mapKey, pushState, 0);

            MapperEndResponse response = master.askSync(
                    new MapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers),
                    ClassTag$.MODULE$.apply(MapperEndResponse.class)
            );
            if (response.status() != StatusCode.Success) {
                String msg = "MapperEnd failed! StatusCode: " + response.status();
                logger.error(msg);
                throw new IOException(msg);
            }
        } finally {
            pushStates.remove(mapKey);
        }
    }

    @Override
    public boolean unregisterShuffle(String applicationId, int shuffleId) {
        UnregisterShuffleResponse response = master.askSync(
            new UnregisterShuffle(applicationId, shuffleId),
            ClassTag$.MODULE$.apply(UnregisterShuffleResponse.class));

        // clear status
        reducePartitionMap.remove(shuffleId);
        reduceFileGroupsMap.remove(shuffleId);

        return response.status().equals(StatusCode.Success);
    }

    @Override
    public boolean stageEnd(String appId, int shuffleId) {
        StageEndResponse response = master.askSync(
            new StageEnd(appId, shuffleId),
            ClassTag$.MODULE$.apply(StageEndResponse.class)
        );
        return response.status() == StatusCode.Success;
    }

    @Override
    public EssInputStream readPartition(
        String applicationId, int shuffleId, int reduceId) throws IOException {
        ReduceFileGroups fileGroups = reduceFileGroupsMap.computeIfAbsent(shuffleId, (id) -> {
            GetReducerFileGroupResponse response = master.askSync(
                    new GetReducerFileGroup(applicationId, shuffleId),
                    ClassTag$.MODULE$.apply(GetReducerFileGroupResponse.class));

            if (response.status() == StatusCode.Success) {
                return new ReduceFileGroups(response.fileGroup(), response.attempts());
            } else {
                logger.error("GetReduceFile Failed! reason " + response.status());
                return null;
            }
        });

        if (fileGroups == null) {
            String msg = "GetReducerFileGroup failed! " +
                    Utils.makeReducerKey(applicationId, shuffleId, reduceId);;
            logger.error(msg);
            throw new IOException(msg);
        }
        if (fileGroups.partitionGroups == null) {
            return EssInputStream.empty();
        }
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
        return EssInputStream.create(conf, dataClientFactory, shuffleKey,
                fileGroups.partitionGroups[reduceId], fileGroups.mapAttempts);
    }

    @Override
    public void startHeartbeat(String applicationId) {
        // start heartbeat if not started
        synchronized (heartBeatLock) {
            if (!heartBeatStarted) {
                Thread heartBeatThread = new Thread() {
                    @Override
                    public void run() {
                        while (!stopped) {
                            master.send(
                                new HeartBeatFromApplication(applicationId)
                            );
                            try {
                                Thread.sleep(30 * 1000);
                            } catch (Exception e) {
                                logger.error("Sleep caught exception!", e);
                            }
                        }
                    }
                };
                heartBeatThread.start();
                heartBeatStarted = true;
            }
        }

    }

    @Override
    public void shutDown() {
        stopped = true;
        rpcEnv.shutdown();
        dataClientFactory.close();
    }
}
