package com.aliyun.emr.jss.client;

import com.aliyun.emr.jss.protocol.PartitionLocation;

import java.io.InputStream;
import java.util.List;

/**
 * ShuffleClient有可能是进程单例
 * 具体的PartitionLocation应该隐藏在实现里面
 */
public abstract class ShuffleClient implements Cloneable
{
    /**
     * 一般在Shuffle启动时候由Driver仅调用一次
     * @param applicationId
     * @param shuffleId
     * @param numMappers
     * @param numPartitions
     * @return
     */
    public abstract boolean registerShuffle(
        String applicationId,
        int shuffleId,
        int numMappers,
        int numPartitions
    );

    /**
     * 当pushData接口异常时，需要调用revive接口来获取一个新的可用的ShuffleWorker
     * 该接口的粒度到具体的一个reduceId
     * 最终一个reduceId可能会对应多个ShuffleWorker
     * @param applicationId
     * @param shuffleId
     * @param reduceId
     * @return
     */
    public abstract boolean revive(
        String applicationId,
        int shuffleId,
        int reduceId
    );

    /**
     * 往具体的一个reduce partition里写数据
     * @param applicationId
     * @param shuffleId
     * @param reduceId
     * @param mapId taskContext.partitionId
     * @param mapAttemptNum taskContext.attemptNumber()
     * @param data
     */
    public abstract void pushData(
        String applicationId,
        int shuffleId,
        int reduceId,
        int mapId,
        int mapAttemptNum,
        byte[] data);

    /**
     * 注销
     * @param applicationId
     * @param shuffleId
     * @return
     */
    public abstract boolean unregisterShuffle(String applicationId, int shuffleId);

    /**
     * reduce端分区读取
     * 按照 mapperId+mapperAttemptNum+batchId 去重
     * batchId是隐藏在实现里的发送时序自增变量
     * @param applicationId
     * @param shuffleId
     * @param reduceId
     * @return
     */
    public abstract InputStream readPartition(
        String applicationId,
        int shuffleId,
        int reduceId
    );

    /**
     * 从Client缓存中提取Shuffle相关的PartitionLocation
     * @param applicationId
     * @param shuffleId
     * @return
     */
    public abstract List<PartitionLocation> fetchShuffleInfo(
        String applicationId, int shuffleId);

    /**
     * 将指定PartitionLocation覆盖Client缓存
     * @param applicationId
     * @param shuffleId
     * @param partitionLocations
     */
    public abstract void applyShuffleInfo(
        String applicationId, int shuffleId, List<PartitionLocation> partitionLocations);
}
