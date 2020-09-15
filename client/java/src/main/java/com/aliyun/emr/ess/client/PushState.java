package com.aliyun.emr.ess.client;

import com.aliyun.emr.ess.protocol.PartitionLocation;
import io.netty.channel.ChannelFuture;
import io.netty.util.internal.ConcurrentSet;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PushState {
    private static final Logger logger = Logger.getLogger(PushState.class);

    public final AtomicInteger batchId = new AtomicInteger();
    public final ConcurrentSet<Integer> inFlightBatches = new ConcurrentSet<>();
    public final ConcurrentHashMap<Integer, ChannelFuture> futures = new ConcurrentHashMap<>();
    public AtomicReference<IOException> exception = new AtomicReference<>();

    public void addFuture(int batchId, ChannelFuture future) {
        futures.put(batchId, future);
    }

    public void removeFuture(int batchId) {
        futures.remove(batchId);
    }

    public synchronized void cancelFutures() {
        if (!futures.isEmpty()) {
            Set<Integer> keys = new HashSet<>(futures.keySet());
            logger.info("cancel futures, size " + keys.size());
            for (Integer batchId : keys) {
                ChannelFuture future = futures.remove(batchId);
                if (future != null) {
                    future.cancel(true);
                }
            }
        }
    }

    // key: ${master addr}-${slave addr} value: list of data batch
    public final ConcurrentHashMap<String, DataBatches> batchesMap = new ConcurrentHashMap<>();

    public void addBatchData(String address, PartitionLocation loc, int batchId, byte[] body) {
       DataBatches batches = batchesMap.computeIfAbsent(address, (s) -> new DataBatches());
       batches.addDataBatch(loc, batchId, body);
    }
}
