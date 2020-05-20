package com.aliyun.emr.jss.service.deploy.master;

import com.aliyun.emr.jss.protocol.PartitionLocation;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MasterUtilSuiteJ {
    @Test
    public void testAllocateSlots() {
        ArrayList<WorkerInfo> workers = new ArrayList<>(3);
        workers.add(new WorkerInfo("worker1", "host1", 10, 1024, null));
        workers.add(new WorkerInfo("worker2", "host2", 11, 1024, null));
        workers.add(new WorkerInfo("worker3", "host3", 12, 1024, null));

        Map<WorkerInfo, Tuple2<List<PartitionLocation>, List<PartitionLocation>>> slots =
                MasterUtil.offerSlots(workers, 12, 3);
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
        assert slots.get(workers.get(0))._1.get(0).getSlavePartitionLocation() != null;
        assert slots.get(workers.get(0))._1.get(0).getSlavePartitionLocation() ==
                slots.get(workers.get(1))._2.get(0);

        assert slots.get(workers.get(0))._2.get(0).getMode() == PartitionLocation.Mode.Slave;
        assert slots.get(workers.get(0))._2.get(0).getHost().equals("host1");
        assert slots.get(workers.get(0))._2.get(0).getPort() == 10;
        assert slots.get(workers.get(0))._2.get(0).getSlavePartitionLocation() == null;

        assert slots.get(workers.get(1))._1.get(0).getMode() == PartitionLocation.Mode.Master;
        assert slots.get(workers.get(1))._1.get(0).getHost().equals("host2");
        assert slots.get(workers.get(1))._1.get(0).getPort() == 11;
        assert slots.get(workers.get(1))._1.get(0).getSlavePartitionLocation() != null;
        assert slots.get(workers.get(1))._1.get(0).getSlavePartitionLocation() ==
                slots.get(workers.get(2))._2.get(0);

        assert slots.get(workers.get(1))._2.get(0).getMode() == PartitionLocation.Mode.Slave;
        assert slots.get(workers.get(1))._2.get(0).getHost().equals("host2");
        assert slots.get(workers.get(1))._2.get(0).getPort() == 11;
        assert slots.get(workers.get(1))._2.get(0).getSlavePartitionLocation() == null;

        assert slots.get(workers.get(2))._1.get(0).getMode() == PartitionLocation.Mode.Master;
        assert slots.get(workers.get(2))._1.get(0).getHost().equals("host3");
        assert slots.get(workers.get(2))._1.get(0).getPort() == 12;
        assert slots.get(workers.get(2))._1.get(0).getSlavePartitionLocation() != null;
        assert slots.get(workers.get(2))._1.get(0).getSlavePartitionLocation() ==
                slots.get(workers.get(0))._2.get(0);

        assert slots.get(workers.get(2))._2.get(0).getMode() == PartitionLocation.Mode.Slave;
        assert slots.get(workers.get(2))._2.get(0).getHost().equals("host3");
        assert slots.get(workers.get(2))._2.get(0).getPort() == 12;
        assert slots.get(workers.get(2))._2.get(0).getSlavePartitionLocation() == null;

        assert workers.get(0).freeMemory() == 1000;
        assert workers.get(1).freeMemory() == 1000;
        assert workers.get(2).freeMemory() == 1000;

        workers.clear();
        workers.add(new WorkerInfo("worker1", "host1", 10, 1024, null));
        workers.add(new WorkerInfo("worker2", "host2", 11, 1024, null));
        workers.add(new WorkerInfo("worker3", "host3", 12, 1024, null));
        slots = MasterUtil.offerSlots(workers, 12, 4);
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
        workers.add(new WorkerInfo("worker1", "host1", 10, 1024, null));
        slots = MasterUtil.offerSlots(workers, 12, 1);
        assert slots == null;

        workers.clear();
        workers.add(new WorkerInfo("worker1", "host1", 10, 1024, null));
        workers.add(new WorkerInfo("worker2", "host2", 10, 1024, null));
        slots = MasterUtil.offerSlots(workers, 1025, 1);
        assert slots == null;

        workers.clear();;
        workers.add(new WorkerInfo("worker1", "host1", 10, 1024, null));
        workers.add(new WorkerInfo("worker2", "host2", 10, 1024, null));
        slots = MasterUtil.offerSlots(workers, 512, 2);
        assert slots.size() == 2;

        workers.clear();;
        workers.add(new WorkerInfo("worker1", "host1", 10, 1024, null));
        workers.add(new WorkerInfo("worker2", "host2", 10, 1024, null));
        slots = MasterUtil.offerSlots(workers, 512, 3);
        assert slots == null;
    }
}
