package com.aliyun.emr.jss.client.impl;

import com.aliyun.emr.jss.client.ShuffleClient;
import com.aliyun.emr.jss.common.EssConf;
import com.aliyun.emr.jss.common.rpc.RpcAddress;
import com.aliyun.emr.jss.common.rpc.RpcEndpointRef;
import com.aliyun.emr.jss.common.rpc.RpcEnv;
import com.aliyun.emr.jss.common.util.Utils;
import com.aliyun.emr.jss.protocol.PartitionLocation;
import com.aliyun.emr.jss.protocol.RpcNameConstants;
import com.aliyun.emr.jss.protocol.message.ControlMessages.*;
import com.aliyun.emr.jss.protocol.message.DataMessages.PushDataResponse;
import com.aliyun.emr.jss.protocol.message.StatusCode;
import com.aliyun.emr.network.buffer.NettyManagedBuffer;
import com.aliyun.emr.network.client.TransportClient;
import com.aliyun.emr.network.protocol.ess.PushData;
import io.netty.buffer.ByteBuf;
import org.apache.log4j.Logger;
import scala.reflect.ClassTag$;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ShuffleClientImpl extends ShuffleClient {

    private static Logger logger = Logger.getLogger(ShuffleClientImpl.class);

    private EssConf conf = null;
    private RpcEnv _env = null;
    private RpcEndpointRef _master = null;
    private Map<String, PartitionLocation> partitionMap = null;
    private Map<PartitionLocation, RpcEndpointRef> workers = new HashMap<>();

    public ShuffleClientImpl() {
        this(new EssConf());
    }

    public ShuffleClientImpl(EssConf conf) {
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
        // TODO 9099 hard code must read from CONF
        _env = RpcEnv.create("ShuffleClient",
            localhost,
            0,
            this.conf);
        _master = _env.setupEndpointRef(new RpcAddress(localhost, 9099), RpcNameConstants.MASTER_EP);

        partitionMap = new ConcurrentHashMap<>();
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
                    Utils.makePartitionKey(applicationId, shuffleId, partitionLoc.getReduceId());
                PartitionLocation prev = partitionMap.putIfAbsent(
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

    @Override
    public boolean revive(String applicationId, int shuffleId, int reduceId) {
        ReviveResponse response = _master.askSync(
            new Revive(applicationId, shuffleId, reduceId),
            ClassTag$.MODULE$.apply(ReviveResponse.class)
        );

        // per partitionKey only serve single PartitionLocation in Client Cache.
        if (response.status().equals(StatusCode.Success)) {
            partitionMap.put(
                Utils.makePartitionKey(applicationId, shuffleId, reduceId),
                response.partitionLocation()
            );
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void pushData(String applicationId, int shuffleId, int reduceId,
        int mapId, int mapAttemptNum, byte[] data) {
        // TODO add logic later
        // pushData might not keep partitionLocation with
    }

    @Override
    public boolean pushData(
        String applicationId,
        int shuffleId,
        int reduceId,
        ByteBuf data) {
        String partitionKey =
            Utils.makePartitionKey(applicationId, shuffleId, reduceId);
        PartitionLocation loc = partitionMap.get(partitionKey);
        return pushData(applicationId, shuffleId, reduceId, loc.getUUID(), data, loc, true);
    }

    @Override
    public boolean unregisterShuffle(String applicationId, int shuffleId) {
        UnregisterShuffleResponse response = _master.askSync(
            new UnregisterShuffle(applicationId, shuffleId),
            ClassTag$.MODULE$.apply(UnregisterShuffleResponse.class)
        );

        if (response.status().equals(StatusCode.Success)) {
            removeAllShufflePartition(applicationId, shuffleId);
            return true;
        } else {
            return false;
        }
    }

    public boolean pushData(String applicationId,
                               int shuffleId,
                               int reduceId,
                               String partitionId,
                               ByteBuf data,
                               PartitionLocation location,
                               boolean firstTry) {
        String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
        PushData pushData = new PushData(
            shuffleKey,
            partitionId,
            PartitionLocation.Mode.Master.mode(),
            new NettyManagedBuffer(data),
            TransportClient.requestId()
        );
        RpcEndpointRef worker = getWorker(location);
        PushDataResponse response = worker.pushDataSync(pushData,
            ClassTag$.MODULE$.apply(PushDataResponse.class)
        );
        // revive and push again if push data failed
        if (response.status() != StatusCode.Success) {
            if (firstTry) {
                boolean success = revive(shuffleKey, shuffleId, reduceId);
                if (!success) {
                    logger.error("Revive failed!");
                    return false;
                }
                // push again
                return pushData(applicationId, shuffleId, reduceId,
                    partitionId, data, location, false);
            } else {
                return false;
            }
        }

        return true;
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
        return null;
    }

    @Override
    public List<PartitionLocation> fetchShuffleInfo(String applicationId, int shuffleId)
    {
        String partitionKeyPrefix = Utils.makeShuffleKey(applicationId, shuffleId);
        return partitionMap.keySet()
            .stream()
            .filter((pk) -> pk.startsWith(partitionKeyPrefix))
            .map((pk) -> partitionMap.get(pk))
            .collect(Collectors.toList());
    }

    @Override
    public void applyShuffleInfo(String applicationId, int shuffleId, List<PartitionLocation> partitionLocations)
    {
        for (int i = 0; i < partitionLocations.size(); i++) {
            PartitionLocation partitionLoc = partitionLocations.get(i);
            partitionMap.put(
                Utils.makePartitionKey(applicationId, shuffleId, partitionLoc.getReduceId()),
                partitionLoc
            );
        }
    }

    private void removeAllShufflePartition(String applicationId, int shuffleId) {
        String partitionKeyPrefix = Utils.makeShuffleKey(applicationId, shuffleId);
        partitionMap.keySet().forEach(tmpPartitionKey -> {
            if (tmpPartitionKey.startsWith(partitionKeyPrefix)) {
                partitionMap.remove(tmpPartitionKey);
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
