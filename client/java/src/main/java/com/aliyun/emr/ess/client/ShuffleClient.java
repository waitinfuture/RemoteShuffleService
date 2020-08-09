package com.aliyun.emr.ess.client;

import com.aliyun.emr.ess.client.impl.ShuffleClientImpl;
import com.aliyun.emr.ess.client.stream.EssInputStream;
import com.aliyun.emr.ess.common.EssConf;

import java.io.IOException;

/**
 * ShuffleClient有可能是进程单例
 * 具体的PartitionLocation应该隐藏在实现里面
 */
public abstract class ShuffleClient implements Cloneable
{
    private static volatile ShuffleClient _instance;

    protected ShuffleClient() {}

    public static ShuffleClient get() {
        return ShuffleClient.get(new EssConf());
    }

    public static ShuffleClient get(EssConf conf) {
        if (null == _instance) {
            synchronized (ShuffleClient.class) {
                if (null == _instance) {
                    _instance = new ShuffleClientImpl(conf);
                }
            }
        }
        return _instance;
    }

    /**
     * 往具体的一个reduce partition里写数据
     * @param applicationId
     * @param shuffleId
     * @param mapId taskContext.partitionId
     * @param attemptId taskContext.attemptNumber()
     * @param reduceId
     * @param data
     * @param offset
     * @param length
     */
    public abstract int pushData(
        String applicationId,
        int shuffleId,
        int mapId,
        int attemptId,
        int reduceId,
        byte[] data,
        int offset,
        int length,
        int numMappers,
        int numPartitions) throws IOException;

    /**
     * report partitionlocations written by the completed map task
     * @param applicationId
     * @param shuffleId
     * @param mapId
     * @param attemptId
     */
    public abstract void mapperEnd(
        String applicationId,
        int shuffleId,
        int mapId,
        int attemptId,
        int numMappers) throws IOException;

    /**
     * cleanup states of the map task
     * @param applicationId
     * @param shuffleId
     * @param mapId
     * @param attemptId
     */
    public abstract void cleanup(String applicationId, int shuffleId, int mapId, int attemptId);

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
    public abstract EssInputStream readPartition(
        String applicationId,
        int shuffleId,
        int reduceId,
        int attemptNumber) throws IOException;

    /**
     * 注销
     * @param applicationId
     * @param shuffleId
     * @return
     */
    public abstract boolean unregisterShuffle(String applicationId, int shuffleId);

    public abstract void startHeartbeat(String applicationId);

    public abstract void shutDown();
}
