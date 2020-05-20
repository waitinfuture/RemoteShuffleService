package com.aliyun.emr.jss.service.deploy.master;

import com.aliyun.emr.jss.protocol.PartitionLocation;
import scala.Array;
import scala.Tuple2;

import javax.swing.plaf.ButtonUI;
import java.util.*;

public class MasterUtil {
    public static Map<WorkerInfo,
            Tuple2<List<PartitionLocation>, List<PartitionLocation>>> offerSlots(
            ArrayList<WorkerInfo> workers,
            int partitionSize,
            int numPartitions) {
        // master partition index
        int masterInd = 0;
        Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
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
            PartitionLocation slavePartition = new PartitionLocation(
                    partitionId,
                    workers.get(nextSlaveInd).host(),
                    workers.get(nextSlaveInd).port(),
                    PartitionLocation.Mode.Slave);
            PartitionLocation masterPartition = new PartitionLocation(
                    partitionId,
                    workers.get(nextMasterInd).host(),
                    workers.get(nextMasterInd).port(),
                    slavePartition);
            workers.get(nextMasterInd).addMasterPartition(partitionId, partitionSize);
            workers.get(nextSlaveInd).addSlavePartition(partitionId, partitionSize);

            // update slots, add master partition location
            slots.putIfAbsent(workers.get(nextMasterInd),
                    new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
            Tuple2<List<PartitionLocation>, List<PartitionLocation>> locations =
                    slots.get(workers.get(nextMasterInd));
            locations._1.add(masterPartition);

            // update slots, add slave partition location
            slots.putIfAbsent(workers.get(nextSlaveInd),
                    new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
            locations = slots.get(workers.get(nextSlaveInd));
            locations._2.add(slavePartition);

            // update index
            masterInd = (nextMasterInd + 1) % workers.size();
        }

        return slots;
    }
}
