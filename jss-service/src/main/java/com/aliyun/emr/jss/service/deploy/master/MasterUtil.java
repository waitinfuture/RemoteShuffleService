package com.aliyun.emr.jss.service.deploy.master;

import com.aliyun.emr.jss.protocol.PartitionLocation;
import scala.Tuple2;

import java.util.*;

public class MasterUtil {
    public static Map<WorkerInfo,
            Tuple2<List<PartitionLocation>, List<String>>> offerSlots(
            ArrayList<WorkerInfo> workers,
            int numPartitions) {
        // master partition index
        int masterInd = 0;
        Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<String>>> slots =
                new HashMap<>();
        // foreach iteration, allocate both master and slave partitions
        for(int p = 0; p < numPartitions; p++) {
            int nextMasterInd = masterInd;
            // try to find slot for master partition
            while (!workers.get(nextMasterInd).slotAvailable()) {
                nextMasterInd = (nextMasterInd + 1) % workers.size();
                if (nextMasterInd == masterInd) {
                    // no available slot
                    return null;
                }
            }
            // try to find slot for slave partition
            int nextSlaveInd = (nextMasterInd + 1) % workers.size();
            while (!workers.get(nextSlaveInd).slotAvailable()) {
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
            Tuple2<List<PartitionLocation>, List<String>> locations =
                    slots.get(workers.get(nextMasterInd));
            PartitionLocation slaveLocation = new PartitionLocation(
                partitionId,
                workers.get(nextSlaveInd).host(),
                workers.get(nextSlaveInd).port(),
                PartitionLocation.Mode.Slave
            );
            PartitionLocation masterLocation = new PartitionLocation(
                partitionId,
                workers.get(nextMasterInd).host(),
                workers.get(nextMasterInd).port(),
                slaveLocation
            );
            workers.get(nextMasterInd).addMasterPartition(partitionId);
            locations._1.add(masterLocation);

            // update slots, add slave partition location
            slots.putIfAbsent(workers.get(nextSlaveInd),
                    new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
            workers.get(nextSlaveInd).addSlavePartition(partitionId);
            locations = slots.get(workers.get(nextSlaveInd));
            locations._2.add(partitionId);

            // update index
            masterInd = (nextMasterInd + 1) % workers.size();
        }

        return slots;
    }

    public static Tuple2<WorkerInfo, String> offerSlaveSlot(
        String partitionId, ArrayList<WorkerInfo> workers) {
        Random rand = new Random();
        int startInd = rand.nextInt(workers.size());
        int curInd;
        for (int i = 0; i < workers.size(); i++) {
            curInd = (startInd + i) % workers.size();
            if (workers.get(curInd).slotAvailable()) {
                return new Tuple2<>(workers.get(curInd), partitionId);
            }
        }
        return null;
    }
}
