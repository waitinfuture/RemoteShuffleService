package com.aliyun.emr.jss.service.deploy.master;

import com.aliyun.emr.jss.protocol.PartitionLocation;
import com.aliyun.emr.jss.service.deploy.worker.WorkerInfo;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MasterUtilSuiteJ {
    @Test
    public void testAllocateSlots() {
        ArrayList<WorkerInfo> workers = new ArrayList<>(3);
        workers.add(new WorkerInfo("host1", 10, 1024, 12, null));
        workers.add(new WorkerInfo("host2", 11, 1024, 12, null));
        workers.add(new WorkerInfo("host3", 12, 1024, 12, null));
        String shuffleKey = "appId-1";

        Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
                MasterUtil.offerSlots(shuffleKey, workers, 3);
        assert slots.size() == 3;

        assert slots.get(workers.get(0))._1.size() == 1;
        assert slots.get(workers.get(0))._2.size() == 1;
        assert slots.get(workers.get(1))._1.size() == 1;
        assert slots.get(workers.get(1))._2.size() == 1;
        assert slots.get(workers.get(2))._1.size() == 1;
        assert slots.get(workers.get(2))._2.size() == 1;

        assert slots.get(workers.get(0))._1.get(0).getMode() == PartitionLocation.Mode.Master;
        assert slots.get(workers.get(0))._1.get(0).getHost().equals("host1");
        assert slots.get(workers.get(0))._1.get(0).getPort() == 10;
        assert slots.get(workers.get(0))._1.get(0).getPeer() != null;
        assert slots.get(workers.get(0))._1.get(0).getPeer().getUUID() ==
                slots.get(workers.get(1))._2.get(0).getUUID();

        assert slots.get(workers.get(1))._1.get(0).getMode() == PartitionLocation.Mode.Master;
        assert slots.get(workers.get(1))._1.get(0).getHost().equals("host2");
        assert slots.get(workers.get(1))._1.get(0).getPort() == 11;
        assert slots.get(workers.get(1))._1.get(0).getPeer() != null;
        assert slots.get(workers.get(1))._1.get(0).getPeer().getUUID() ==
                slots.get(workers.get(2))._2.get(0).getUUID();

        assert slots.get(workers.get(2))._1.get(0).getMode() == PartitionLocation.Mode.Master;
        assert slots.get(workers.get(2))._1.get(0).getHost().equals("host3");
        assert slots.get(workers.get(2))._1.get(0).getPort() == 12;
        assert slots.get(workers.get(2))._1.get(0).getPeer() != null;
        assert slots.get(workers.get(2))._1.get(0).getPeer().getUUID() ==
                slots.get(workers.get(0))._2.get(0).getUUID();

        assert workers.get(0).freeMemory() == 1000;
        assert workers.get(1).freeMemory() == 1000;
        assert workers.get(2).freeMemory() == 1000;

        workers.clear();
        workers.add(new WorkerInfo("host1", 10, 1024, 12, null));
        workers.add(new WorkerInfo("host2", 11, 1024, 12, null));
        workers.add(new WorkerInfo("host3", 12, 1024, 12, null));
        slots = MasterUtil.offerSlots(shuffleKey, workers, 4);
        assert slots.size() == 3;
        assert slots.get(workers.get(0))._1.size() == 2;
        assert slots.get(workers.get(0))._2.size() == 1;
        assert slots.get(workers.get(1))._1.size() == 1;
        assert slots.get(workers.get(1))._2.size() == 2;
        assert slots.get(workers.get(2))._1.size() == 1;
        assert slots.get(workers.get(2))._2.size() == 1;

        assert workers.get(0).freeMemory() == 988;
        assert workers.get(1).freeMemory() == 988;
        assert workers.get(2).freeMemory() == 1000;

        workers.clear();
        workers.add(new WorkerInfo("host1", 10, 1024, 12, null));
        slots = MasterUtil.offerSlots(shuffleKey, workers, 1);
        assert slots == null;

        workers.clear();
        workers.add(new WorkerInfo("host1", 10, 1024, 1025, null));
        workers.add(new WorkerInfo("host2", 10, 1024, 1025, null));
        slots = MasterUtil.offerSlots(shuffleKey, workers, 1);
        assert slots == null;

        workers.clear();;
        workers.add(new WorkerInfo("host1", 10, 1024, 32, null));
        workers.add(new WorkerInfo("host2", 10, 1024, 32, null));
        slots = MasterUtil.offerSlots(shuffleKey, workers, 2);
        assert slots.size() == 2;

        workers.clear();;
        workers.add(new WorkerInfo("host1", 10, 1024, 512, null));
        workers.add(new WorkerInfo("host2", 10, 1024, 512, null));
        slots = MasterUtil.offerSlots(shuffleKey, workers, 3);
        assert slots == null;
    }
}
