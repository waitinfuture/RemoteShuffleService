package com.aliyun.emr.ess.client.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import scala.reflect.ClassTag$;

import com.google.common.collect.Lists;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.util.internal.ConcurrentSet;
import org.apache.log4j.Logger;

import com.aliyun.emr.ess.client.DataBatches;
import com.aliyun.emr.ess.client.PushState;
import com.aliyun.emr.ess.client.ShuffleClient;
import com.aliyun.emr.ess.client.compress.EssLz4Compressor;
import com.aliyun.emr.ess.client.stream.EssInputStream;
import com.aliyun.emr.ess.common.EssConf;
import com.aliyun.emr.ess.common.rpc.RpcAddress;
import com.aliyun.emr.ess.common.rpc.RpcEndpointRef;
import com.aliyun.emr.ess.common.rpc.RpcEnv;
import com.aliyun.emr.ess.common.util.ThreadUtils;
import com.aliyun.emr.ess.common.util.Utils;
import com.aliyun.emr.ess.protocol.PartitionLocation;
import com.aliyun.emr.ess.protocol.RpcNameConstants;
import com.aliyun.emr.ess.protocol.TransportModuleConstants;
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
import com.aliyun.emr.network.protocol.PushMergedData;
import com.aliyun.emr.network.server.NoOpRpcHandler;
import com.aliyun.emr.network.util.TransportConf;

public class ShuffleClientImpl extends ShuffleClient {
    private static final Logger logger = Logger.getLogger(ShuffleClientImpl.class);

    private static final byte MASTER_MODE = PartitionLocation.Mode.Master.mode();

    private static final Random rand = new Random();

    private final EssConf conf;
    private final int maxInFlight;
    private final int MergedDataSizeThreshold;

    private RpcEnv rpcEnv;
    private RpcEndpointRef master;

    private TransportClientFactory dataClientFactory;

    private InetAddress ia = null;

    // key: shuffleId, value: (reduceId, PartitionLocation)
    private final Map<Integer, ConcurrentHashMap<Integer, PartitionLocation>> reducePartitionMap =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, ConcurrentSet<String>> mapperEndMap = new ConcurrentHashMap<>();

    // key: shuffleId-mapId-attemptId
    private final Map<String, PushState> pushStates = new ConcurrentHashMap<>();

    private ExecutorService pushDataRetryPool;

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
        MergedDataSizeThreshold = EssConf.essMergePushDataThreshold(conf);
        logger.info("MergedDataSizeThreshold: " + MergedDataSizeThreshold);
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

        TransportConf dataTransportConf = Utils.fromEssConf(
            conf,
            TransportModuleConstants.DATA_MODULE,
            conf.getInt("ess.data.io.threads", 8));
        TransportContext context =
            new TransportContext(dataTransportConf, new NoOpRpcHandler(), true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        dataClientFactory = context.createClientFactory(bootstraps);

        int retryThreadNum = EssConf.essPushDataRetryThreadNum(conf);
        pushDataRetryPool = ThreadUtils.newDaemonCachedThreadPool("Retry-Sender", retryThreadNum, 60);
    }

    private void submitRetryPushData(String applicationId,
                                     int shuffleId,
                                     int mapId,
                                     int attemptId,
                                     byte[] body,
                                     int batchId,
                                     PartitionLocation loc,
                                     RpcResponseCallback callback,
                                     PushState pushState) {
        int reduceId = loc.getReduceId();
        if (!revive(applicationId, shuffleId, mapId, attemptId, reduceId,
            loc.getEpoch(), loc)) {
            callback.onFailure(new IOException("Revive Failed"));
        } else if (mapperEnded(shuffleId, mapId, attemptId)) {
            pushState.inFlightBatches.remove(batchId);
        } else {
            PartitionLocation newLoc = reducePartitionMap.get(shuffleId).get(reduceId);
            logger.info("revive success " + newLoc);
            try {
                TransportClient client = dataClientFactory.createClient(
                    newLoc.getHost(), newLoc.getPort(), reduceId);
                NettyManagedBuffer newBuffer =
                    new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
                String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
                PushData newPushData =
                    new PushData(MASTER_MODE, shuffleKey, newLoc.getUniqueId(), newBuffer);
                ChannelFuture future = client.pushData(newPushData, callback);
                pushState.addFuture(batchId, future);
            } catch (Exception ex) {
                callback.onFailure(ex);
            }
        }
    }

    private void submitRetryPushMergedData(PushState pushState,
                                           String applicationId,
                                           int shuffleId,
                                           int mapId,
                                           int attemptId,
                                           ArrayList<DataBatches.DataBatch> batches,
                                           boolean revived) {
        HashMap<String, DataBatches> newDataBatchesMap = new HashMap<>();
        for (DataBatches.DataBatch batch : batches) {
            int reduceId = batch.loc.getReduceId();
            if (!revive(applicationId, shuffleId, mapId, attemptId,
                reduceId, batch.loc.getEpoch(), batch.loc)) {
                pushState.exception.compareAndSet(null,
                    new IOException("Revive Failed in retry push merged data" +
                        "for location: " + batch.loc));
                return;
            } else if (mapperEnded(shuffleId, mapId, attemptId)) {
                pushState.inFlightBatches.remove(batch.batchId);
            } else {
                PartitionLocation newLoc = reducePartitionMap.get(shuffleId).get(reduceId);
                DataBatches newDataBaches = newDataBatchesMap.computeIfAbsent(genAddressPair(newLoc),
                    (s) -> new DataBatches());
                newDataBaches.addDataBatch(newLoc, batch.batchId, batch.body);
            }
        }

        for (Map.Entry<String, DataBatches> entry : newDataBatchesMap.entrySet()) {
            String addressPair = entry.getKey();
            DataBatches newDataBatches = entry.getValue();
            String[] tokens = addressPair.split("-");
            doPushMergedData(tokens[0], applicationId, shuffleId, mapId, attemptId,
                newDataBatches.requireBatches(), pushState, revived);
        }
    }

    private String genAddressPair(PartitionLocation loc) {
        String addressPair;
        if (loc.getPeer() != null) {
            addressPair = loc.hostPort() + "-" + loc.getPeer().hostPort();
        } else {
            addressPair = loc.hostPort();
        }
        return addressPair;
    }

    private ConcurrentHashMap<Integer, PartitionLocation> registerShuffle(
        String applicationId, int shuffleId, int numMappers, int numPartitions) {
        if (!heartBeatStarted) {
            startHeartbeat(applicationId);
        }
        int numRetries = 3;
        while (numRetries > 0) {
            try {
                RegisterShuffleResponse response = master.askSync(
                    new RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions, getLocalHost()),
                    ClassTag$.MODULE$.apply(RegisterShuffleResponse.class)
                );

                if (response.status().equals(StatusCode.Success)) {
                    ConcurrentHashMap<Integer, PartitionLocation> result = new ConcurrentHashMap<>();
                    for (int i = 0; i < response.partitionLocations().size(); i++) {
                        PartitionLocation partitionLoc = response.partitionLocations().get(i);
                        result.put(partitionLoc.getReduceId(), partitionLoc);
                    }
                    return result;
                }
            } catch (Exception e) {
                logger.error("registerShuffle failed", e);
                break;
            }

            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                break;
            }
            numRetries--;
        }

        return null;
    }

    private void limitMaxInFlight(
        String mapKey, PushState pushState, int limit) throws IOException {
        if (pushState.exception.get() != null) {
            throw pushState.exception.get();
        }

        ConcurrentSet<Integer> inFlightBatches = pushState.inFlightBatches;
        long delta = EssConf.essLimitInFlightSleepDeltaMs(conf);
        long times = EssConf.essLimitInFlightTimeoutMs(conf) / delta;
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

        if (times <= 0) {
            for (int req : inFlightBatches) {
                logger.error(mapKey + " batch " + req);
            }
            throw new IOException("wait timeout for task " + mapKey, pushState.exception.get());
        }
        if (pushState.exception.get() != null) {
            throw pushState.exception.get();
        }
    }

    private boolean waitRevivedLocation(
        ConcurrentHashMap<Integer, PartitionLocation> map, int reduceId, int epoch) {
        PartitionLocation currentLocation = map.get(reduceId);
        if (currentLocation != null && currentLocation.getEpoch() > epoch) {
            return true;
        }

        long sleepTimeMs = rand.nextInt(50);
        if (sleepTimeMs > 30) {
            try {
                TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
            } catch (InterruptedException e) {
                logger.warn("waitRevivedLocation interrupted", e);
                Thread.currentThread().interrupt();
            }
        }

        currentLocation = map.get(reduceId);
        if (currentLocation != null && currentLocation.getEpoch() > epoch) {
            return true;
        }
        return false;
    }

    private boolean revive(
        String applicationId,
        int shuffleId,
        int mapId,
        int attemptId,
        int reduceId,
        int epoch,
        PartitionLocation oldLocation) {
        ConcurrentHashMap<Integer, PartitionLocation> map = reducePartitionMap.get(shuffleId);
        if (waitRevivedLocation(map, reduceId, epoch)) {
            logger.info("[revive] already revived, just return: " + shuffleId + "-" + mapId + "-" +
                reduceId + "-" + epoch);
            return true;
        }
        String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
        if (mapperEnded(shuffleId, mapId, attemptId)) {
            logger.info("[revive] Mapper already ended, just return: " + shuffleId + "-" + mapId +
                "-" + reduceId + "-" + epoch);
            return true;
        }

        try {
            ReviveResponse response = master.askSync(
                new Revive(applicationId, shuffleId, mapId, attemptId, reduceId, epoch, oldLocation),
                ClassTag$.MODULE$.apply(ReviveResponse.class)
            );

            // per partitionKey only serve single PartitionLocation in Client Cache.
            if (response.status().equals(StatusCode.Success)) {
                map.put(reduceId, response.partitionLocation());
                return true;
            } else if (response.status().equals(StatusCode.MapEnded)) {
                mapperEndMap.computeIfAbsent(shuffleId, (id) -> new ConcurrentSet<>())
                    .add(mapKey);
                return true;
            } else {
                logger.error("revive failed: " + shuffleId + "-" + reduceId + "-" + epoch);
                return false;
            }
        } catch (Exception e) {
            logger.error("revive exception: "+ shuffleId + "-" + reduceId + "-" + epoch
                + "," + e.toString());
            return false;
        }
    }

    public int pushOrMergeData(
        String applicationId,
        int shuffleId,
        int mapId,
        int attemptId,
        int reduceId,
        byte[] data,
        int offset,
        int length,
        int numMappers,
        int numPartitions,
        boolean doPush) throws IOException {
        // mapKey
        final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
        // return if shuffle stage already ended
        if (mapperEnded(shuffleId, mapId, attemptId)) {
            PushState pushState = pushStates.get(mapKey);
            if (pushState != null) {
                pushState.cancelFutures();
            }
            return 0;
        }
        // register shuffle if not registered
        final ConcurrentHashMap<Integer, PartitionLocation> map =
            reducePartitionMap.computeIfAbsent(shuffleId, (id) ->
                registerShuffle(applicationId, shuffleId, numMappers, numPartitions));

        if (map == null) {
            String msg = "registerShuffle failed! shuffleKey "
                + Utils.makeShuffleKey(applicationId, shuffleId);
            throw new IOException(msg);
        }

        // get location
        if (!map.containsKey(reduceId) &&
            !revive(applicationId, shuffleId, mapId, attemptId, reduceId, 0, null)) {
            String msg = "revive failed! shuffleKey "
                + Utils.makeShuffleKey(applicationId, shuffleId) + ", reduceId " + reduceId;
            throw new IOException(msg);
        }
        final PartitionLocation loc = map.get(reduceId);
        if (loc == null) {
            String msg = "loc is NULL! shuffleKey "
                + Utils.makeShuffleKey(applicationId, shuffleId) + ", reduceId " + reduceId;
            throw new IOException(msg);
        }

        PushState pushState = pushStates.computeIfAbsent(mapKey, (s) -> new PushState());

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

        if (doPush) {
            // check limit
            limitMaxInFlight(mapKey, pushState, maxInFlight);

            // add inFlight requests
            pushState.inFlightBatches.add(nextBatchId);

            // build PushData request
            final String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
            NettyManagedBuffer buffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
            PushData pushData = new PushData(MASTER_MODE, shuffleKey, loc.getUniqueId(), buffer);

            // build callback
            RpcResponseCallback callback = new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    pushState.inFlightBatches.remove(nextBatchId);
                    if (response.remaining() > 0 &&
                        response.get() == StatusCode.StageEnded.getValue()) {
                        mapperEndMap.computeIfAbsent(shuffleId, (id) -> new ConcurrentSet<>())
                            .add(mapKey);
                    }
                    pushState.removeFuture(nextBatchId);
                }

                @Override
                public void onFailure(Throwable e) {
                    pushState.exception.compareAndSet(null,
                        new IOException("Revived PushData failed!", e));
                    pushState.removeFuture(nextBatchId);
                }
            };

            RpcResponseCallback wrappedCallback = new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    callback.onSuccess(response);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (pushState.exception.get() != null) {
                        return;
                    }
                    // async retry pushdata
                    if (!mapperEnded(shuffleId, mapId, attemptId)) {
                        logger.warn("PushData failed, taskId " + mapId + ", attemptId " + attemptId +
                            ",reduceId " + reduceId + ",batchId " + nextBatchId +
                            ", put into retry queue, cause: " + e.toString());
                        pushDataRetryPool.submit(() ->
                            submitRetryPushData(applicationId, shuffleId, mapId, attemptId, body,
                                nextBatchId, loc, callback, pushState));
                    }
                }
            };

            // do push data
            try {
                TransportClient client =
                    dataClientFactory.createClient(loc.getHost(), loc.getPort(), reduceId);
                ChannelFuture future = client.pushData(pushData, wrappedCallback);
                pushState.addFuture(nextBatchId, future);
            } catch (Exception e) {
                wrappedCallback.onFailure(e);
            }
        } else {
            // add batch data
            pushState.addBatchData(genAddressPair(loc), loc, nextBatchId, body);
        }

        return body.length;
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

        return pushOrMergeData(applicationId, shuffleId, mapId, attemptId, reduceId,
            data, offset, length, numMappers, numPartitions, true);
    }

    @Override
    public void prepareForMergeData(int shuffleId,
                                    int mapId,
                                    int attemptId) throws IOException {
        final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
        PushState pushState = pushStates.get(mapKey);
        if (pushState != null) {
            limitMaxInFlight(mapKey, pushState, 0);
        }
    }

    @Override
    public int mergeData(
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

        return pushOrMergeData(applicationId, shuffleId, mapId, attemptId, reduceId,
            data, offset, length, numMappers, numPartitions, false);
    }

    public void pushMergedData(String applicationId,
                               int shuffleId,
                               int mapId,
                               int attemptId) throws IOException {
        final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
        PushState pushState = pushStates.get(mapKey);
        if (pushState == null) {
            return;
        }
        ArrayList<Map.Entry<String, DataBatches>> batchesArr =
            new ArrayList<>(pushState.batchesMap.entrySet());
        while (!batchesArr.isEmpty()) {
            limitMaxInFlight(mapKey, pushState, maxInFlight);
            Map.Entry<String, DataBatches> entry = batchesArr.get(rand.nextInt(batchesArr.size()));
            ArrayList<DataBatches.DataBatch> batches =
                entry.getValue().requireBatches(MergedDataSizeThreshold);
            if (entry.getValue().getTotalSize() == 0) {
                batchesArr.remove(entry);
            }
            for (DataBatches.DataBatch batch : batches) {
                pushState.inFlightBatches.add(batch.batchId);
            }
            String[] tokens = entry.getKey().split("-");
            doPushMergedData(tokens[0], applicationId, shuffleId, mapId, attemptId,
                batches, pushState, false);
        }
    }

    private void doPushMergedData(String hostPort,
                                  String applicationId,
                                  int shuffleId,
                                  int mapId,
                                  int attemptId,
                                  ArrayList<DataBatches.DataBatch> batches,
                                  PushState pushState,
                                  boolean revived) {
        final String[] splits = hostPort.split(":");
        final String host = splits[0];
        final int port = Integer.parseInt(splits[1]);

        final int numBatches = batches.size();
        final String[] partitionUniqueIds = new String[numBatches];
        final int[] offsets = new int[numBatches];
        final int[] batchIds = new int[numBatches];
        int currentSize = 0;
        CompositeByteBuf byteBuf = Unpooled.compositeBuffer();
        for (int i = 0; i < numBatches; i++) {
            DataBatches.DataBatch batch = batches.get(i);
            partitionUniqueIds[i] = batch.loc.getUniqueId();
            offsets[i] = currentSize;
            batchIds[i] = batch.batchId;
            currentSize += batch.body.length;
            byteBuf.addComponent(true, Unpooled.wrappedBuffer(batch.body));
        }
        NettyManagedBuffer buffer = new NettyManagedBuffer(byteBuf);
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
        PushMergedData mergedData =
            new PushMergedData(MASTER_MODE, shuffleKey, partitionUniqueIds, offsets, buffer);

        RpcResponseCallback callback = new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                for (int batchId : batchIds) {
                    pushState.inFlightBatches.remove(batchId);
                }
                if (response.remaining() > 0 &&
                    response.get() == StatusCode.StageEnded.getValue()) {
                    mapperEndMap.computeIfAbsent(shuffleId, (id) -> new ConcurrentSet<>())
                        .add(Utils.makeMapKey(shuffleId, mapId, attemptId));
                }
            }

            @Override
            public void onFailure(Throwable e) {
                pushState.exception.compareAndSet(null,
                    new IOException("Revived PushMergedData failed!", e));
            }
        };

        RpcResponseCallback wrappedCallback = new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                callback.onSuccess(response);
            }

            @Override
            public void onFailure(Throwable e) {
                if (pushState.exception.get() != null) {
                    return;
                }
                if (revived) {
                    callback.onFailure(e);
                    return;
                }
                if (!mapperEnded(shuffleId, mapId, attemptId)) {
                    logger.warn("PushMergedData failed, mtaskId " + mapId + ", attemptId " +
                        attemptId + ", put into retry queue, " + e.toString());
                    pushDataRetryPool.submit(() ->
                        submitRetryPushMergedData(pushState, applicationId, shuffleId, mapId,
                            attemptId, batches, true));
                }
            }
        };

        // do push merged data
        try {
            TransportClient client =
                dataClientFactory.createClient(host, port);
            client.pushMergedData(mergedData, wrappedCallback);
        } catch (Exception e) {
            wrappedCallback.onFailure(e);
        }
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
    public void cleanup(String applicationId, int shuffleId, int mapId, int attemptId) {
        final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
        PushState pushState = pushStates.remove(mapKey);
        if (pushState != null) {
            pushState.exception.compareAndSet(null, new IOException("Cleaned Up"));
            pushState.cancelFutures();
        }
    }

    @Override
    public boolean unregisterShuffle(String applicationId, int shuffleId, boolean isDriver) {
        if (isDriver) {
            try {
                master.send(new UnregisterShuffle(applicationId, shuffleId));
            } catch (Exception e) {
                logger.error("Send UnregisterShuffle failed, ignore", e);
            }
        }

        // clear status
        reducePartitionMap.remove(shuffleId);
        reduceFileGroupsMap.remove(shuffleId);
        mapperEndMap.remove(shuffleId);

        return true;
    }

    @Override
    public EssInputStream readPartition(
        String applicationId, int shuffleId, int reduceId, int attemptNumber) throws IOException {
        ReduceFileGroups fileGroups = reduceFileGroupsMap.computeIfAbsent(shuffleId, (id) -> {
            GetReducerFileGroupResponse response = master.askSync(
                    new GetReducerFileGroup(applicationId, shuffleId),
                    ClassTag$.MODULE$.apply(GetReducerFileGroupResponse.class));

            if (response.status() == StatusCode.Success) {
                return new ReduceFileGroups(response.fileGroup(), response.attempts());
            } else {
                logger.error("Get ReduceFileGroups Failed! reason " + response.status());
                return null;
            }
        });

        if (fileGroups == null) {
            String msg = "Shuffle data lost! " +
                    Utils.makeReducerKey(applicationId, shuffleId, reduceId);;
            logger.error(msg);
            throw new IOException(msg);
        }
        if (fileGroups.partitionGroups == null) {
            return EssInputStream.empty();
        }
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
        return EssInputStream.create(conf, dataClientFactory, shuffleKey,
                fileGroups.partitionGroups[reduceId], fileGroups.mapAttempts, attemptNumber);
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
                            master.send(new HeartBeatFromApplication(applicationId));
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
        pushDataRetryPool.shutdown();
    }

    private synchronized String getLocalHost() {
        if (ia == null) {
            try {
                ia = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                logger.error("Unknown host", e);
                return null;
            }
        }
        return ia.getHostName();
    }

    private boolean mapperEnded(int shuffleId, int mapId, int attemptId) {
        return mapperEndMap.containsKey(shuffleId) &&
            mapperEndMap.get(shuffleId).contains(Utils.makeMapKey(shuffleId, mapId, attemptId));
    }
}