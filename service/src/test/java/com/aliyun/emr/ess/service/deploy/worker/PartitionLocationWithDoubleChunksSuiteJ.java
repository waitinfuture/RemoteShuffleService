package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.ess.protocol.PartitionLocation;
import org.junit.Test;

import java.util.*;

public class PartitionLocationWithDoubleChunksSuiteJ {
    @Test
    public void testEquals() {
        List<PartitionLocationWithDoubleChunks> list = new ArrayList<>();
        PartitionLocation p1 = new PartitionLocation(0, "p1", "host1", 10, PartitionLocation.Mode.Slave);
        PartitionLocation p2 = new PartitionLocation(1, "p2", "host1", 11, PartitionLocation.Mode.Slave);
        PartitionLocationWithDoubleChunks pd1 = new PartitionLocationWithDoubleChunks(p1, null);
        PartitionLocationWithDoubleChunks pd2 = new PartitionLocationWithDoubleChunks(p2, null);
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
            new PartitionLocation(0, "p1", "host1", 10, PartitionLocation.Mode.Slave);
        assert map.containsKey(p);

        map.remove(p1);
        assert map.size() == 1;
        map.put(p2, p2);
        assert map.size() == 1;

        Map<PartitionLocation, PartitionLocation> map2 = new HashMap<>();
        PartitionLocation p3 = new PartitionLocationWithDoubleChunks(
            new PartitionLocation(
                2,
                "3fe66fbc-a588-40e8-8755-fd5255dfca15",
                "30.225.12.48",
                9097,
                PartitionLocation.Mode.Master
            ),
            null
        );
        map2.put(p3, p3);
        PartitionLocation p4 = new PartitionLocation(
            2,
            "3fe66fbc-a588-40e8-8755-fd5255dfca15",
            "30.225.12.48",
            9097,
            PartitionLocation.Mode.Slave
        );
        assert map2.containsKey(p4);
    }
}
