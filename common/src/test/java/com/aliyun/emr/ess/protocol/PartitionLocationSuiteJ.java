package com.aliyun.emr.ess.protocol;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionLocationSuiteJ {
    @Test
    public void testEquals() {
        PartitionLocation l1 = new PartitionLocation(
            0, 0, "localhost", 1, PartitionLocation.Mode.Master);
        PartitionLocation l2 = new PartitionLocation(
            0, 1, "localhost", 1, PartitionLocation.Mode.Slave);
        assert(l1.equals(l2));

        Map<PartitionLocation, String> map = new HashMap<>();
        map.put(l1, "p1");
        map.remove(l2);
        assert map.size() == 0;

        map.put(l1, "p1");
        map.put(l2, "p2");
        assert map.size() == 1;
        assert map.get(l2).equals("p2");
        assert map.get(l1).equals("p2");

        List<PartitionLocation> list = new ArrayList<>();
        list.add(l1);
        list.add(l2);
        assert list.size() == 2;
        list.remove(l1);
        assert list.size() == 1;
        list.remove(l1);
        assert list.size() == 0;

        list.add(l1);
        assert list.contains(l2);

        list.add(l1);
        list.add(l2);
        List<PartitionLocation> list2 = new ArrayList<>();
        list2.add(l1);
        list2.add(l2);
        list.removeAll(list2);
        assert list.size() == 0;
    }
}
