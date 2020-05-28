package com.aliyun.emr.jss.service.deploy.master;

import com.aliyun.emr.jss.protocol.PartitionLocation;
import com.aliyun.emr.jss.service.deploy.worker.WorkerInfo;
import scala.Tuple2;

import java.util.*;

public class MasterUtil {
    public static void releaseSlots(String shuffleKey, Map<WorkerInfo,
        Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots) {
        Iterator<WorkerInfo> workers = slots.keySet().iterator();
        while (workers.hasNext()) {
            WorkerInfo worker = workers.next();
            Tuple2<List<PartitionLocation>, List<PartitionLocation>> allocatedSlots =
                slots.get(worker);
            worker.removeMasterPartition(shuffleKey, allocatedSlots._1);
            worker.removeSlavePartition(shuffleKey, allocatedSlots._2);
        }
    }

    public static Map<WorkerInfo,
        Tuple2<List<PartitionLocation>, List<PartitionLocation>>> offerSlots(
            String shuffleKey,
            List<WorkerInfo> workers,
            int numPartitions) {
        // master partition index
        int masterInd = 0;
        Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
                new HashMap<>();
        // foreach iteration, allocate both master and slave partitions
        for(int p = 0; p < numPartitions; p++) {
            int nextMasterInd = masterInd;
            // try to find slot for master partition
            while (!workers.get(nextMasterInd).slotAvailable()) {
                nextMasterInd = (nextMasterInd + 1) % workers.size();
                if (nextMasterInd == masterInd) {
                    // no available slot, release allocated resource
                    releaseSlots(shuffleKey, slots);
                    return null;
                }
            }
            // try to find slot for slave partition
            int nextSlaveInd = (nextMasterInd + 1) % workers.size();
            while (!workers.get(nextSlaveInd).slotAvailable()) {
                nextSlaveInd = (nextSlaveInd + 1) % workers.size();
                if (nextSlaveInd == nextMasterInd) {
                    // no available slot, release allocated resource
                    releaseSlots(shuffleKey, slots);
                    return null;
                }
            }
            if (nextSlaveInd == nextMasterInd) {
                // no available slot, release allocated resource
                releaseSlots(shuffleKey, slots);
                return null;
            }
            // now nextMasterInd/nextSlaveInd point to
            // available master/slave partition respectively
            String partitionId = UUID.randomUUID().toString();
//            String partitionId = "aaa";

            // new slave and master locations
            slots.putIfAbsent(workers.get(nextMasterInd),
                    new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
            Tuple2<List<PartitionLocation>, List<PartitionLocation>> locations =
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
                PartitionLocation.Mode.Master,
                slaveLocation
            );
            slaveLocation.setPeer(masterLocation);

            // add master location to WorkerInfo
            workers.get(nextMasterInd).addMasterPartition(shuffleKey, masterLocation);
            locations._1.add(masterLocation);

            // add slave location to WorkerInfo
            slots.putIfAbsent(workers.get(nextSlaveInd),
                    new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
            locations = slots.get(workers.get(nextSlaveInd));
            workers.get(nextSlaveInd).addSlavePartition(shuffleKey, slaveLocation);
            locations._2.add(slaveLocation);

            // update index
            masterInd = (nextMasterInd + 1) % workers.size();
        }

        return slots;
    }

    public static Tuple2<WorkerInfo, PartitionLocation> offerSlaveSlot(
        PartitionLocation masterLocation, List<WorkerInfo> workers) {
        Random rand = new Random();
        int startInd = rand.nextInt(workers.size());
        if (workers.get(startInd).hostPort().equals(masterLocation.hostPort())) {
            startInd = (startInd + 1) % workers.size();
        }
        int curInd;
        for (int i = 0; i < workers.size() - 1; i++) {
            curInd = (startInd + i) % workers.size();
            if (workers.get(curInd).slotAvailable()) {
                PartitionLocation location = new PartitionLocation(
                    masterLocation.getUUID(),
                    workers.get(curInd).host(),
                    workers.get(curInd).port(),
                    PartitionLocation.Mode.Slave,
                    masterLocation
                );
                return new Tuple2<>(workers.get(curInd), location);
            }
        }
        return null;
    }
}
