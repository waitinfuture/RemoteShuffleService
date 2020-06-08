package com.aliyun.emr.jss.client;

import com.aliyun.emr.jss.protocol.PartitionLocation;
import com.aliyun.emr.jss.protocol.message.StatusCode;
import io.netty.buffer.ByteBuf;

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
     * 往具体的一个reduce partition里写数据
     * @param applicationId
     * @param shuffleId
     * @param mapId taskContext.partitionId
     * @param attempId taskContext.attemptNumber()
     * @param reduceId
     * @param data
     */
    public abstract boolean pushData(
        String applicationId,
        int shuffleId,
        int mapId,
        int attempId,
        int reduceId,
        byte[] data);

    public abstract boolean pushData(
        String applicationId,
        int shuffleId,
        int mapId,
        int attempId,
        int reduceId,
        byte[] data,
        int offset,
        int length);

    public abstract boolean pushData(
        String applicationId,
        int shuffleId,
        int mapId,
        int attempId,
        int reduceId,
        ByteBuf data);

    /**
     * report partitionlocations written by the completed map task
     * @param applicationId
     * @param shuffleId
     * @param mapId
     * @param attempId
     * @return
     */
    public abstract boolean mapperEnd(
        String applicationId,
        int shuffleId,
        int mapId,
        int attempId
    );

    /**
     * commit files, update status
     * @param appId
     * @param shuffleId
     * @return
     */
    public abstract boolean stageEnd(String appId, int shuffleId);

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
     * 注销
     * @param applicationId
     * @param shuffleId
     * @return
     */
    public abstract boolean unregisterShuffle(String applicationId, int shuffleId);

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

    public abstract void shutDown();
}
