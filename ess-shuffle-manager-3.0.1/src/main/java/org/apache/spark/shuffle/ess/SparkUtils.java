package org.apache.spark.shuffle.ess;

import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.storage.BlockManagerId;

public class SparkUtils {
    public static MapStatus createMapStatus(
            BlockManagerId loc, long[] uncompressedSizes, long mapTaskId) {
        return MapStatus$.MODULE$.apply(loc, uncompressedSizes, mapTaskId);
    }
}
