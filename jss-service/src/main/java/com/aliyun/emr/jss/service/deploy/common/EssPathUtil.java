package com.aliyun.emr.jss.service.deploy.common;

import com.aliyun.emr.jss.common.EssConf;
import org.apache.hadoop.fs.Path;

public class EssPathUtil
{
    public static String workerBaseDir(EssConf conf) {
        return conf.get("ess.worker.base.dir", "file:///tmp/Aliyun-EMR-SS/");
    }

    public static Path GetBaseDir(EssConf conf) {
        return new Path(String.format("%s/", workerBaseDir(conf)));
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
