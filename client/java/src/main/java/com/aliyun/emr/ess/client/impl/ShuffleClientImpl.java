package com.aliyun.emr.ess.client.impl;

import com.aliyun.emr.ess.client.ShuffleClient;
import com.aliyun.emr.ess.client.compress.EssLz4Compressor;
import com.aliyun.emr.ess.client.stream.EssInputStream;
import com.aliyun.emr.ess.common.EssConf;
import com.aliyun.emr.ess.common.rpc.RpcAddress;
import com.aliyun.emr.ess.common.rpc.RpcEndpointRef;
import com.aliyun.emr.ess.common.rpc.RpcEnv;
import com.aliyun.emr.ess.common.util.EssPathUtil;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
    // key: appId-shuffleId-mapId-attemptId
    private final Map<String, PushState> pushStates = new ConcurrentHashMap<>();
    ThreadLocal<EssLz4Compressor> lz4CompressorThreadLocal = new ThreadLocal<EssLz4Compressor>() {
        @Override
        protected EssLz4Compressor initialValue() {
            int blockSize = (int) EssConf.essPushDataBufferSize(conf);
            return new EssLz4Compressor(blockSize);
        }
    };

    // key: appId-shuffleId-reduceId   value: file path set
    private final Map<String, HashSet<String>> reducerFileGroup = new HashMap<>();
    // key: appId-shuffleId    value: attempts
    private final Map<String, int[]> mapAttempts = new HashMap<>();
    // filesystem for read partition
    FileSystem fs;

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
        maxInFlight = EssConf.essPushDataMaxInflight(conf);
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

        Configuration hadoopConf = new Configuration();
        Path path = EssPathUtil.GetBaseDir(conf);
        try {
            fs = path.getFileSystem(hadoopConf);
        } catch (IOException e) {
            logger.error("GetFileSystem failed!", e);
        }
    }

    @Override
    public StatusCode registerShuffle(String applicationId, int shuffleId, int numMappers, int numPartitions) {
        // registerShuffle
        RegisterShuffleResponse response = master.askSync(
            new RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions),
            ClassTag$.MODULE$.apply(RegisterShuffleResponse.class)
        );

        if (response.status().equals(StatusCode.Success)) {
            ConcurrentHashMap<Integer, PartitionLocation> map = reducePartitionMap.get(shuffleId);
            synchronized (map) {
                for (int i = 0; i < response.partitionLocations().size(); i++) {
                    PartitionLocation partitionLoc = response.partitionLocations().get(i);
                    map.put(partitionLoc.getReduceId(), partitionLoc);
                }
            }
            return StatusCode.Success;
        } else if (response.status().equals(StatusCode.NumMapperZero)) {
            return StatusCode.Success;
        } else {
            return response.status();
        }
    }

    private void limitMaxInFlight(String mapKey, PushState pushState, int limit)
            throws IOException {
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
                if (pushState.exception != null) {
                    throw pushState.exception.get();
                }
                Thread.sleep(delta);
                times--;
            }
        } catch (Exception e) {
            logger.error("sleep caught exception");
        }
        if (inFlightBatches.size() > limit) {
            for (int req : inFlightBatches) {
                logger.error(mapKey + " batch " + req);
            }
            throw new IOException("wait timeout for task " + mapKey, pushState.exception.get());
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
        final String mapKey = Utils.makeMapKey(applicationId, shuffleId, mapId, attemptId);
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

        // register shuffle if not registered
        synchronized (reducePartitionMap) {
            reducePartitionMap.putIfAbsent(shuffleId, new ConcurrentHashMap<>());
        }
        ConcurrentHashMap<Integer, PartitionLocation> map = reducePartitionMap.get(shuffleId);
        synchronized (map) {
            if (!map.containsKey(reduceId)) {
                StatusCode status = registerShuffle(applicationId, shuffleId, numMappers, numPartitions);
                if (status != StatusCode.Success) {
                    throw new IOException("Register shuffle failed! status: " + status);
                }
            }
        }

        // get location
        PartitionLocation loc = reducePartitionMap.get(shuffleId).get(reduceId);
        if (loc == null) {
            String msg = "loc is NULL! shuffleKey" + Utils.makeShuffleKey(applicationId, shuffleId) + ", reduceId " + reduceId;
            throw new IOException(msg);
        }

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
                        TransportClient client = dataClientFactory.createClient(newLoc.getHost(), newLoc.getPort());
                        NettyManagedBuffer newBuffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
                        PushData newPushData = new PushData(MASTER_MODE, shuffleKey, newLoc.getUniqueId(), newBuffer);
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
                dataClientFactory.createClient(loc.getHost(), loc.getPort());
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
        final String mapKey = Utils.makeMapKey(applicationId, shuffleId, mapId, attemptId);
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
            ClassTag$.MODULE$.apply(UnregisterShuffleResponse.class)
        );

        // clear status
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
        reducePartitionMap.remove(shuffleId);
        synchronized (reducerFileGroup) {
            Set<String> keys = new HashSet<>(reducerFileGroup.keySet());
            for (String key : keys) {
                if (key.startsWith(Utils.shuffleKeyPrefix(shuffleKey))) {
                    reducerFileGroup.remove(key);
                }
            }
        }
        synchronized (mapAttempts) {
            mapAttempts.remove(shuffleKey);
        }

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
    public InputStream readPartition(
        String applicationId, int shuffleId, int reduceId) throws IOException {
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
        String reducerKey = Utils.makeReducerKey(applicationId, shuffleId, reduceId);
        synchronized (reducerFileGroup) {
            if (!reducerFileGroup.containsKey(reducerKey)) {
                GetReducerFileGroupResponse response = master.askSync(
                    new GetReducerFileGroup(applicationId, shuffleId),
                    ClassTag$.MODULE$.apply(GetReducerFileGroupResponse.class)
                );
                if (response.status() != StatusCode.Success) {
                    String msg = "GetReducerFileGroup failed! status: " + response.status();
                    logger.error(msg);
                    throw new IOException(msg);
                }
                // update reducerFileGroup
                if (response.fileGroup() != null) {
                    reducerFileGroup.putAll(response.fileGroup());
                }
                // update attempts
                logger.debug("get fileGroup success, shuflfleKey " + shuffleKey +
                    "mapAttempts " + Arrays.toString(response.attempts()));
                if (response.attempts() != null) {
                    mapAttempts.put(shuffleKey, response.attempts());
                }
            } else {
                if (!mapAttempts.containsKey(shuffleKey)) {
                    logger.error("reducerFileGroup contains " + reducerKey +
                        " but mapAttempts doesn't contain " + shuffleKey);
                } else {
                    logger.debug("reducerFileGroup contains " + reducerKey +
                        " and mapAttempts contains " + shuffleKey);
                }
            }
        }

        HashSet<String> files;
        int[] attempts;
        synchronized (reducerFileGroup) {
            files = reducerFileGroup.getOrDefault(reducerKey, new HashSet<>());
            attempts = mapAttempts.getOrDefault(shuffleKey, null);
        }
        EssInputStream inputStream = new EssInputStream(conf, files, attempts);
        if (attempts == null) {
            logger.warn("mapAttempts doesn't contains " + shuffleKey);
        }

        return inputStream;
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
