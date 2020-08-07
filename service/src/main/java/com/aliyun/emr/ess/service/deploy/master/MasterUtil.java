package com.aliyun.emr.ess.service.deploy.master;

import com.aliyun.emr.ess.protocol.PartitionLocation;
import com.aliyun.emr.ess.service.deploy.worker.WorkerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

public class MasterUtil {
    private static final Logger logger = LoggerFactory.getLogger(MasterUtil.class);

    private static final Random rand = new Random();

    public static void releaseSlots(String shuffleKey, Map<WorkerInfo,
        Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots) {
        Iterator<WorkerInfo> workers = slots.keySet().iterator();
        while (workers.hasNext()) {
            WorkerInfo worker = workers.next();
            Tuple2<List<PartitionLocation>, List<PartitionLocation>> allocatedSlots =
                slots.get(worker);
            HashSet<String> ids = new HashSet<>();
            for (int i = 0; i < allocatedSlots._1.size(); i++) {
                ids.add(allocatedSlots._1.get(i).getUniqueId());
            }
            worker.removeMasterPartitions(shuffleKey, ids);
            ids.clear();
            for (int i = 0; i < allocatedSlots._2.size(); i++) {
                ids.add(allocatedSlots._2.get(i).getUniqueId());
            }
            worker.removeSlavePartitions(shuffleKey, ids);
        }
    }

    public static Map<WorkerInfo,
        Tuple2<List<PartitionLocation>, List<PartitionLocation>>> offerSlots(
        String shuffleKey,
        List<WorkerInfo> workers,
        List<Integer> reduceIds) {
        int[] oldEpochs = new int[reduceIds.size()];
        Arrays.fill(oldEpochs, -1);
        return offerSlots(shuffleKey, workers, reduceIds, oldEpochs);
    }

    public static Map<WorkerInfo,
        Tuple2<List<PartitionLocation>, List<PartitionLocation>>> offerSlots(
            String shuffleKey,
            List<WorkerInfo> workers,
            List<Integer> reduceIds,
            int[] oldEpochs) {
        logger.info("inside offerSlots, reduceId num " + reduceIds.size());

        if (workers.size() < 2) {
            logger.error("offerSlots failed: require at least 2 active workers");
            return null;
        }

        int masterInd = rand.nextInt(workers.size());
        Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
                new HashMap<>();
        // foreach iteration, allocate both master and slave partitions
        for(int idx = 0; idx < reduceIds.size(); idx++) {
            int nextMasterInd = masterInd;
            // try to find slot for master partition
            while (!workers.get(nextMasterInd).slotAvailable()) {
                nextMasterInd = (nextMasterInd + 1) % workers.size();
                if (nextMasterInd == masterInd) {
                    // no available slot, release allocated resource
                    releaseSlots(shuffleKey, slots);
                    logger.error("No available slot for master");
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
                    logger.error("No available slot for slave #0");
                    return null;
                }
            }
            if (nextSlaveInd == nextMasterInd) {
                // no available slot, release allocated resource
                releaseSlots(shuffleKey, slots);
                logger.error("No available slot for slave #1");
                return null;
            }
            // now nextMasterInd/nextSlaveInd point to
            // available master/slave partition respectively

            int newEpoch = oldEpochs[idx] + 1;
            // new slave and master locations
            slots.putIfAbsent(workers.get(nextMasterInd),
                    new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
            Tuple2<List<PartitionLocation>, List<PartitionLocation>> locations =
                    slots.get(workers.get(nextMasterInd));
            PartitionLocation slaveLocation = new PartitionLocation(
                reduceIds.get(idx),
                newEpoch,
                workers.get(nextSlaveInd).host(),
                workers.get(nextSlaveInd).port(),
                PartitionLocation.Mode.Slave
            );
            PartitionLocation masterLocation = new PartitionLocation(
                reduceIds.get(idx),
                newEpoch,
                workers.get(nextMasterInd).host(),
                workers.get(nextMasterInd).port(),
                PartitionLocation.Mode.Master,
                slaveLocation
            );
            slaveLocation.setPeer(masterLocation);

            // add master location to WorkerInfo
            WorkerInfo worker = workers.get(nextMasterInd);
            worker.addMasterPartition(shuffleKey, masterLocation);
            locations._1.add(masterLocation);

            // add slave location to WorkerInfo
            slots.putIfAbsent(workers.get(nextSlaveInd),
                    new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
            locations = slots.get(workers.get(nextSlaveInd));
            worker = workers.get(nextSlaveInd);
            worker.addSlavePartition(shuffleKey, slaveLocation);
            locations._2.add(slaveLocation);

            // update index
            masterInd = (nextMasterInd + 1) % workers.size();
        }

        return slots;
    }

    public static Tuple2<WorkerInfo, PartitionLocation> offerSlaveSlot(
        PartitionLocation masterLocation, List<WorkerInfo> workers) {
        int startInd = rand.nextInt(workers.size());
        if (workers.get(startInd).hostPort().equals(masterLocation.hostPort())) {
            startInd = (startInd + 1) % workers.size();
        }
        int curInd;
        for (int i = 0; i < workers.size() - 1; i++) {
            curInd = (startInd + i) % workers.size();
            if (workers.get(curInd).slotAvailable()) {
                PartitionLocation location = new PartitionLocation(
                    masterLocation.getReduceId(),
                    masterLocation.getEpoch(),
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
