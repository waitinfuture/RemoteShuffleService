package com.aliyun.emr.ess.common.util;

import com.aliyun.emr.ess.common.EssConf;
import com.aliyun.emr.ess.protocol.PartitionLocation;
import org.apache.hadoop.fs.Path;

public class EssPathUtil
{
    public static String workerBaseDir(EssConf conf) {
        return EssConf.essWorkerBaseDir(conf);
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

    public static Path GetPartitionPath(EssConf conf, String appId, int shuffleId, int reduceId, int epoch,
                                        PartitionLocation.Mode mode) {
        String suffix;
        if (mode == PartitionLocation.Mode.Master) {
            suffix = "m";
        } else {
            suffix = "s";
        }
        return new Path(String.format("%s/%s/%s/%s/%s-%s", workerBaseDir(conf), appId, shuffleId, reduceId, epoch, suffix));
    }
}
