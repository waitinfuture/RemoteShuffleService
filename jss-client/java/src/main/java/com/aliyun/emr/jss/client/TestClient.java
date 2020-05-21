package com.aliyun.emr.jss.client;

import com.aliyun.emr.jss.client.impl.ShuffleClientImpl;
import com.aliyun.emr.jss.protocol.PartitionLocation;

import java.util.List;

public class TestClient
{
    public static void main(String[] args) {
        ShuffleClientImpl client = new ShuffleClientImpl();
        client.init();
        List<PartitionLocation> result = client.registerShuffle("appId", 1, 20, 10);
        if (result != null) {
            for (int i = 0; i < result.size(); i++) {
                System.out.println(result.get(i));
            }
        }


    }
}
