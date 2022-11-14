package org.apache.tez.shuffle.ess;

import com.aliyun.emr.ess.client.ShuffleClient;
import com.aliyun.emr.ess.common.EssConf;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class EssShuffleManager {

    private static final Logger logger = LoggerFactory.getLogger(EssShuffleManager.class);
    private final String appId;
    private final EssConf essConf;
    private final ShuffleClient essShuffleClient;

    public EssShuffleManager(
        String appId,
        EssConf essConf) {
        this.appId = appId;
        this.essConf = essConf;
        this.essShuffleClient = ShuffleClient.get(essConf);
    }

    public void registerShuffle() {
        essShuffleClient.startHeartbeat(appId);
    }

    public EssShuffleWriter getWriter(
        int taskAttemptId,
        int shuffleId,
        int mapId,
        int numMappers,
        int numPartitions,
        Class keyClass,
        Class valClass) throws IOException {
        return new EssShuffleWriter(
            appId,
            shuffleId,
            mapId,
            taskAttemptId,
            numMappers,
            numPartitions,
            keyClass,
            valClass,
            essConf
        );
    }

    // TODO getReader
    public EssShuffleReader getReader(
        int taskAttemptId,
        int shuffleId,
        int startPartition,
        int endPartition,
        boolean emptyShuffle) throws IOException {
        return new EssShuffleReader(
            appId,
            shuffleId,
            startPartition,
            endPartition,
            emptyShuffle,
            taskAttemptId,
            essConf);
    }

    public void unregisterShuffle(int shuffleId, boolean isDriver) {
        essShuffleClient.unregisterShuffle(appId, shuffleId, isDriver);
    }

    public EssConf getConf() {
        return essConf;
    }

    public static EssConf fromTezConf(Configuration conf) {
        EssConf tmpEssConf = new EssConf();
        Map<String, String> properties = conf.getPropsWithPrefix("tez.runtime.");
        properties.forEach((k, v) -> {
            if (k.startsWith("ess")) tmpEssConf.set(k, v);
        });
        return tmpEssConf;
    }
}
