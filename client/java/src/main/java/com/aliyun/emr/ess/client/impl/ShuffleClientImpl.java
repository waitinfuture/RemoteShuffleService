package com.aliyun.emr.ess.client.impl;

import com.aliyun.emr.ess.client.MetricsCallback;
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
import com.aliyun.emr.ess.protocol.message.ControlMessages.GetReducerFileGroup;
import com.aliyun.emr.ess.protocol.message.ControlMessages.GetReducerFileGroupResponse;
import com.aliyun.emr.ess.protocol.message.ControlMessages.MapperEnd;
import com.aliyun.emr.ess.protocol.message.ControlMessages.MapperEndResponse;
import com.aliyun.emr.ess.protocol.message.ControlMessages.RegisterShuffle;
import com.aliyun.emr.ess.protocol.message.ControlMessages.RegisterShuffleResponse;
import com.aliyun.emr.ess.protocol.message.ControlMessages.Revive;
import com.aliyun.emr.ess.protocol.message.ControlMessages.ReviveResponse;
import com.aliyun.emr.ess.protocol.message.ControlMessages.StageEnd;
import com.aliyun.emr.ess.protocol.message.ControlMessages.StageEndResponse;
import com.aliyun.emr.ess.protocol.message.ControlMessages.UnregisterShuffle;
import com.aliyun.emr.ess.protocol.message.ControlMessages.UnregisterShuffleResponse;
import com.aliyun.emr.ess.protocol.message.DataMessages.PushDataResponse;
import com.aliyun.emr.ess.protocol.message.StatusCode;
import com.aliyun.emr.network.buffer.NettyManagedBuffer;
import com.aliyun.emr.network.client.TransportClient;
import com.aliyun.emr.network.protocol.ess.PushData;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ShuffleClientImpl extends ShuffleClient {
    private static Logger logger = Logger.getLogger(ShuffleClientImpl.class);

    private EssConf conf;
    private RpcEnv _env = null;
    private RpcEndpointRef _master = null;
    // key: appId-shuffleId-reduceId   value: PartitionLocation
    private Map<String, PartitionLocation> reducePartitionMap = null;
    // key: appId-shuffleId-reduceId   value: file path set
    private Map<String, HashSet<String>> reducerFileGroup = new HashMap<>();
    // key: appId-shuffleId    value: attempts
    private Map<String, int[]> mapAttempts = new HashMap<>();
    // key: appId-shuffleId-mapId-attemptId  value: partitions written
    public Map<String, HashSet<PartitionLocation>> mapWrittenPartitions = new ConcurrentHashMap<>();
    // key: appId-shuffleId-mapId-attemptId  value: batchId
    private Map<String, AtomicInteger> mapBatchIds = new ConcurrentHashMap<>();
    private Map<PartitionLocation, RpcEndpointRef> workers = new HashMap<>();

    FileSystem fs;

    ThreadLocal<EssLz4Compressor> lz4CompressorThreadlocal = new ThreadLocal<EssLz4Compressor>() {
        @Override
        protected EssLz4Compressor initialValue() {
            int blockSize = (int) EssConf.essPushDataBufferSize(conf);
            return new EssLz4Compressor(blockSize);
        }
    };

    public ShuffleClientImpl() {
        this(new EssConf());
    }

    public ShuffleClientImpl(EssConf conf) {
        super();
        this.conf = conf;
        init();
    }

    public void init() {
        // init rpc env and master endpointRef
        String localhost = Utils.localHostName();
        _env = RpcEnv.create("ShuffleClient",
            Utils.localHostName(),
            0,
            conf);
        _env = RpcEnv.create("ShuffleClient",
            localhost,
            0,
            this.conf);
        _master = _env.setupEndpointRef(
            new RpcAddress(
                EssConf.essMasterHost(conf),
                EssConf.essMasterPort(conf))
            , RpcNameConstants.MASTER_EP);

        reducePartitionMap = new ConcurrentHashMap<>();
        Configuration hadoopConf = new Configuration();
        Path path = EssPathUtil.GetBaseDir(conf);
        try {
            fs = path.getFileSystem(hadoopConf);
        } catch (IOException e) {
            logger.error("GetFileSystem failed!", e);
        }
    }

    @Override
    public boolean registerShuffle(String applicationId, int shuffleId, int numMappers, int numPartitions) {
        // registerShuffle
        RegisterShuffleResponse response = _master.askSync(
            new RegisterShuffle(applicationId, shuffleId, numMappers, numPartitions),
            ClassTag$.MODULE$.apply(RegisterShuffleResponse.class)
        );

        if (response.status().equals(StatusCode.Success)) {
            for (int i = 0; i < response.partitionLocations().size(); i++) {
                PartitionLocation partitionLoc = response.partitionLocations().get(i);
                String partitionKey =
                    Utils.makeReducerKey(applicationId, shuffleId, partitionLoc.getReduceId());
                PartitionLocation prev = reducePartitionMap.putIfAbsent(
                    partitionKey,
                    partitionLoc
                );

                // if prev != null, means there are illegal partitionKey already exist
                // consider register shuffle failed and clean up
                if (prev != null) {
                    removeAllShufflePartition(applicationId, shuffleId);
                    logger.error(String.format("Illegal partitionKey %s already exists.", partitionKey));
                    return false;
                }
            }

            // return true 的时候基本意味所有的master location已经加入到本地缓存
            return true;
        } else {
            return false;
        }
    }

    private boolean revive(String applicationId, int shuffleId, int reduceId) {
        ReviveResponse response = _master.askSync(
            new Revive(applicationId, shuffleId, reduceId),
            ClassTag$.MODULE$.apply(ReviveResponse.class)
        );

        // per partitionKey only serve single PartitionLocation in Client Cache.
        if (response.status().equals(StatusCode.Success)) {
            reducePartitionMap.put(
                Utils.makeReducerKey(applicationId, shuffleId, reduceId),
                response.partitionLocation()
            );
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean pushData(String applicationId,
                            int shuffleId,
                            int mapId,
                            int attemptId,
                            int reduceId,
                            byte[] data) {
        return pushData(applicationId, shuffleId, mapId, attemptId, reduceId, data, 0, data.length);
    }

    @Override
    public boolean pushData(String applicationId,
                            int shuffleId,
                            int mapId,
                            int attemptId,
                            int reduceId,
                            byte[] data,
                            int offset,
                            int length) {
        // TODO add logic later
        // pushData might not keep partitionLocation with

        // increment batchId
        String mapKey = Utils.makeMapKey(applicationId, shuffleId, mapId, attemptId);
        if (!mapBatchIds.containsKey(mapKey)) {
            mapBatchIds.put(mapKey, new AtomicInteger());
        }
        AtomicInteger batchId = mapBatchIds.get(mapKey);
        int nextBatchId = batchId.addAndGet(1);

        // compress data
        EssLz4Compressor compressor = lz4CompressorThreadlocal.get();
        compressor.compress(data, offset, length);

        // copy compressed to ByteBuffer
        // TODO optimize ByteBuffer
        int compressedTotalSize = compressor.getCompressedTotalSize();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 * 4 + compressedTotalSize);
        byteBuffer.putInt(mapId);
        byteBuffer.putInt(attemptId);
        byteBuffer.putInt(nextBatchId);
        byteBuffer.putInt(compressedTotalSize);
        byteBuffer.put(compressor.getCompressedBuffer(), 0, compressedTotalSize);
        byteBuffer.flip();

        ByteBuf buf = Unpooled.wrappedBuffer(byteBuffer);

        return pushData(applicationId, shuffleId, mapId, attemptId, reduceId, buf, nextBatchId);
    }

    @Override
    public boolean pushData(
        String applicationId,
        int shuffleId,
        int mapId,
        int attemptId,
        int reduceId,
        ByteBuf data,
        int batchId) {
        String partitionKey =
            Utils.makeReducerKey(applicationId, shuffleId, reduceId);
        PartitionLocation loc = reducePartitionMap.get(partitionKey);
        String id = mapId + "-" + attemptId + "-" + batchId;
        boolean res = pushData(applicationId, shuffleId, reduceId, data, loc, true, id);
        // update mapWrittenPartitions
        if (res) {
            String mapKey = Utils.makeMapKey(applicationId, shuffleId, mapId, attemptId);
            if (!mapWrittenPartitions.containsKey(mapKey)) {
                if (!mapWrittenPartitions.containsKey(mapKey)) {
                    HashSet<PartitionLocation> locations = new HashSet<>();
                    mapWrittenPartitions.put(mapKey, locations);
                }
            }
            Set<PartitionLocation> locations = mapWrittenPartitions.get(mapKey);
            locations.add(loc);
        }
        return res;
    }

    public boolean pushData(String applicationId,
                            int shuffleId,
                            int reduceId,
                            ByteBuf data,
                            PartitionLocation location,
                            boolean firstTry,
                            String batchId) {
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
        PushData pushData = new PushData(
            shuffleKey,
            location.getUUID(),
            PartitionLocation.Mode.Master.mode(),
            new NettyManagedBuffer(data),
            TransportClient.requestId(),
            batchId
        );
        RpcEndpointRef worker = getWorker(location);
        PushDataResponse response = worker.pushDataSync(pushData,
            ClassTag$.MODULE$.apply(PushDataResponse.class)
        );
        // revive and push again if push data failed
        if (response.status() != StatusCode.Success) {
            if (firstTry) {
                boolean success = revive(applicationId, shuffleId, reduceId);
                if (!success) {
                    logger.error("Revive failed!");
                    return false;
                }
                // push again
                return pushData(applicationId, shuffleId, reduceId,
                    data, location, false, batchId);
            } else {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean mapperEnd(
        String applicationId,
        int shuffleId,
        int mapId,
        int attemptId
    ) {
        String mapKey = Utils.makeMapKey(applicationId, shuffleId, mapId, attemptId);
        HashSet<PartitionLocation> locations = mapWrittenPartitions.getOrDefault(mapKey, new
            HashSet<>());
        MapperEndResponse response = _master.askSync(
            new MapperEnd(applicationId, shuffleId, mapId, attemptId, new ArrayList<>
                (locations)),
            ClassTag$.MODULE$.apply(MapperEndResponse.class)
        );
        if (response.status() != StatusCode.Success) {
            logger.error("MapperEnd failed! StatusCode: " + response.status());
            return false;
        }
        return true;
    }

    @Override
    public boolean unregisterShuffle(String applicationId, int shuffleId) {
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
        UnregisterShuffleResponse response = _master.askSync(
            new UnregisterShuffle(applicationId, shuffleId),
            ClassTag$.MODULE$.apply(UnregisterShuffleResponse.class)
        );

        // clear status
        Set<String> keys = reducePartitionMap.keySet();
        keys.forEach(key -> {
            if (key.startsWith(Utils.shuffleKeyPrefix(shuffleKey))) {
                reducePartitionMap.remove(key);
            }
        });
        synchronized (reducerFileGroup) {
            keys = new HashSet<>(reducerFileGroup.keySet());
            for (String key : keys) {
                if (key.startsWith(Utils.shuffleKeyPrefix(shuffleKey))) {
                    reducerFileGroup.remove(key);
                }
            }
        }
        keys = mapWrittenPartitions.keySet();
        keys.forEach(key -> {
            if (key.startsWith(Utils.shuffleKeyPrefix(shuffleKey))) {
                mapWrittenPartitions.remove(key);
            }
        });

        if (response.status().equals(StatusCode.Success)) {
            removeAllShufflePartition(applicationId, shuffleId);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean stageEnd(String appId, int shuffleId) {
        StageEndResponse response = _master.askSync(
            new StageEnd(appId, shuffleId),
            ClassTag$.MODULE$.apply(StageEndResponse.class)
        );
        return response.status() == StatusCode.Success;
    }

    @Override
    public InputStream readPartition(String applicationId, int shuffleId, int reduceId) {
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
        String reducerKey = Utils.makeReducerKey(applicationId, shuffleId, reduceId);
        synchronized (reducerFileGroup) {
            if (!reducerFileGroup.containsKey(reducerKey)) {
                GetReducerFileGroupResponse response = _master.askSync(
                    new GetReducerFileGroup(applicationId, shuffleId),
                    ClassTag$.MODULE$.apply(GetReducerFileGroupResponse.class)
                );
                if (response.status() != StatusCode.Success) {
                    logger.error("GetReducerFileGroup failed! status: " + response.status());
                    return null;
                }
                // update reducerFileGroup
                reducerFileGroup.putAll(response.fileGroup());
                // update attempts
                logger.debug("get fileGroup success, shuflfleKey " + shuffleKey +
                    "mapAttempts " + response.attempts());
                mapAttempts.put(shuffleKey, response.attempts());
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
        EssInputStream inputStream = null;
        if (attempts != null) {
            inputStream = new EssInputStream(
                conf,
                files,
                mapAttempts.get(shuffleKey)
            );
        } else {
            logger.error("mapAttempts doesn't contains " + shuffleKey);
        }

        return inputStream;
    }

    @Override
    public List<PartitionLocation> fetchShuffleInfo(String applicationId, int shuffleId) {
        String partitionKeyPrefix = Utils.makeShuffleKey(applicationId, shuffleId);
        return reducePartitionMap.keySet()
            .stream()
            .filter((pk) -> pk.startsWith(partitionKeyPrefix))
            .map((pk) -> reducePartitionMap.get(pk))
            .collect(Collectors.toList());
    }

    @Override
    public void applyShuffleInfo(String applicationId, int shuffleId, List<PartitionLocation> partitionLocations) {
        for (int i = 0; i < partitionLocations.size(); i++) {
            PartitionLocation partitionLoc = partitionLocations.get(i);
            reducePartitionMap.put(
                Utils.makeReducerKey(applicationId, shuffleId, partitionLoc.getReduceId()),
                partitionLoc
            );
        }
    }

    private void removeAllShufflePartition(String applicationId, int shuffleId) {
        String partitionKeyPrefix = Utils.makeShuffleKey(applicationId, shuffleId);
        reducePartitionMap.keySet().forEach(tmpPartitionKey -> {
            if (tmpPartitionKey.startsWith(partitionKeyPrefix)) {
                reducePartitionMap.remove(tmpPartitionKey);
            }
        });
    }

    @Override
    public void shutDown() {
        _env.shutdown();
    }

    private RpcEndpointRef getWorker(PartitionLocation loc) {
        if (workers.containsKey(loc)) {
            return workers.get(loc);
        }
        synchronized (workers) {
            if (workers.containsKey(loc)) {
                return workers.get(loc);
            }
            RpcEndpointRef worker = _env.setupEndpointRef(
                new RpcAddress(loc.getHost(), loc.getPort()), RpcNameConstants.WORKER_EP);
            workers.put(loc, worker);
            return worker;
        }
    }
}
