package com.aliyun.emr.jss.service.deploy.master;

import com.aliyun.emr.jss.protocol.PartitionLocation;
import scala.Tuple2;

import java.util.*;

public class MasterUtil {
    public static Map<WorkerInfo,
            Tuple2<List<String>, List<String>>> offerSlots(
            ArrayList<WorkerInfo> workers,
            int partitionSize,
            int numPartitions) {
        // master partition index
        int masterInd = 0;
        Map<WorkerInfo, Tuple2<List<String>, List<String>>> slots =
                new HashMap<>();
        // foreach iteration, allocate both master and slave partitions
        for(int p = 0; p < numPartitions; p++) {
            int nextMasterInd = masterInd;
            // try to find slot for master partition
            while (workers.get(nextMasterInd).freeMemory() < partitionSize) {
                nextMasterInd = (nextMasterInd + 1) % workers.size();
                if (nextMasterInd == masterInd) {
                    // no available slot
                    return null;
                }
            }
            // try to find slot for slave partition
            int nextSlaveInd = (nextMasterInd + 1) % workers.size();
            while (workers.get(nextSlaveInd).freeMemory() < partitionSize) {
                nextSlaveInd = (nextSlaveInd + 1) % workers.size();
                if (nextSlaveInd == nextMasterInd) {
                    // no available slot
                    return null;
                }
            }
            if (nextSlaveInd == nextMasterInd) {
                // no available slot
                return null;
            }
            // now nextMasterInd/nextSlaveInd point to
            // available master/slave partition respectively
            String partitionId = UUID.randomUUID().toString();

            // update slots, add master partition location
            slots.putIfAbsent(workers.get(nextMasterInd),
                    new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
            Tuple2<List<String>, List<String>> locations =
                    slots.get(workers.get(nextMasterInd));
            locations._1.add(partitionId);

            // update slots, add slave partition location
            slots.putIfAbsent(workers.get(nextSlaveInd),
                    new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
            locations = slots.get(workers.get(nextSlaveInd));
            locations._2.add(partitionId);

            // update index
            masterInd = (nextMasterInd + 1) % workers.size();
        }

        return slots;
    }

    public static Tuple2<WorkerInfo, PartitionLocation> offerSlot(
        String partitionId, ArrayList<WorkerInfo> workers, int partitionSize) {
        Random rand = new Random();
        int startInd = rand.nextInt(workers.size());
        int curInd;
        for (int i = 0; i < workers.size(); i++) {
            curInd = (startInd + i) % workers.size();
            if (workers.get(curInd).freeMemory() > partitionSize) {
                workers.get(curInd).addSlavePartition(partitionId, partitionSize);
                PartitionLocation location = new PartitionLocation(
                    partitionId,
                    workers.get(curInd).host(),
                    workers.get(curInd).port(),
                    PartitionLocation.Mode.Slave
                );
                return new Tuple2<>(workers.get(curInd), location);
            }
        }
        return null;
    }
}
