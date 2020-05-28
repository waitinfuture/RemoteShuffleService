package com.aliyun.emr.jss.service.deploy.common;

import com.aliyun.emr.jss.common.EssConf;
import org.apache.hadoop.fs.Path;

public class EssPathUtil
{
    private static String workerBaseDir(EssConf conf) {
        return conf.get("ess.worker.base.dir", "file:///tmp/Aliyun-EMR-SS/");
    }

    public static String GetShuffleKey(String appId, int shuffleId) {
        return String.format("%s-%s", appId, shuffleId);
    }

    public static Path GetAppDir(EssConf conf, String appId) {
        return new Path(String.format("%s/%s/", workerBaseDir(conf), appId));
    }

    public static Path GetShuffleDir(EssConf conf, String appId, int shuffleId) {
        return new Path(String.format("%s/%s/%s/", workerBaseDir(conf), appId, shuffleId));
    }

    public static Path GetPartitionPath(EssConf conf, String appId, int shuffleId, String partitionId) {
        return new Path(String.format("%s/%s/%s/%s", workerBaseDir(conf), appId, shuffleId, partitionId));
    }
}
