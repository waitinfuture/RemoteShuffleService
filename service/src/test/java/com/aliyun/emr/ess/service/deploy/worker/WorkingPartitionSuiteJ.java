package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.ess.protocol.PartitionLocation;
import org.junit.Test;

import java.util.*;

public class WorkingPartitionSuiteJ {
    @Test
    public void testEquals() {
        List<WorkingPartition> list = new ArrayList<>();
        PartitionLocation p1 = new PartitionLocation(0, 0, "host1", 10, PartitionLocation.Mode.Slave);
        PartitionLocation p2 = new PartitionLocation(1, 1, "host1", 11, PartitionLocation.Mode.Slave);
        WorkingPartition pd1 = new WorkingPartition(p1, null);
        WorkingPartition pd2 = new WorkingPartition(p2, null);
        list.add(pd1);
        list.add(pd2);
        assert list.size() == 2;
        list.remove(p1);
        assert list.size() == 1;
        list.remove(p2);
        assert list.size() == 0;

        Map<PartitionLocation, PartitionLocation> map = new HashMap<>();
        map.put(pd1, pd1);
        map.put(pd2, pd2);

        PartitionLocation p =
            new PartitionLocation(0, 0, "host1", 10, PartitionLocation.Mode.Slave);
        assert map.containsKey(p);

        map.remove(p1);
        assert map.size() == 1;
        map.put(p2, p2);
        assert map.size() == 1;

        Map<PartitionLocation, PartitionLocation> map2 = new HashMap<>();
        PartitionLocation p3 = new WorkingPartition(
            new PartitionLocation(
                2,
                1,
                "30.225.12.48",
                9097,
                PartitionLocation.Mode.Master
            ),
            null
        );
        map2.put(p3, p3);
        PartitionLocation p4 = new PartitionLocation(
            2,
            1,
            "30.225.12.48",
            9097,
            PartitionLocation.Mode.Slave
        );
        assert map2.containsKey(p4);
    }
}
