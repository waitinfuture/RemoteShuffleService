package com.aliyun.emr.ess.client;

import com.aliyun.emr.ess.protocol.PartitionLocation;

import java.util.ArrayList;

public class DataBatches {
    private int totalSize = 0;
    private ArrayList<DataBatch> batches = new ArrayList<>();

    public static class DataBatch {
        public final PartitionLocation loc;
        public final int batchId;
        public final byte[] body;

        public DataBatch(PartitionLocation loc, int batchId, byte[] body) {
            this.loc = loc;
            this.batchId = batchId;
            this.body = body;
        }
    }

    public synchronized void addDataBatch(PartitionLocation loc, int batchId, byte[] body) {
        DataBatch dataBatch = new DataBatch(loc, batchId, body);
        batches.add(dataBatch);
        totalSize += body.length;
    }

    public int getTotalSize() {
        return totalSize;
    }

    public ArrayList<DataBatch> requireBatches() {
        totalSize = 0;
        return batches;
    }

    public ArrayList<DataBatch> requireBatches(int requestSize) {
        if (requestSize >= totalSize) {
            totalSize = 0;
            return batches;
        }
        ArrayList<DataBatch> retBatches = new ArrayList<>();
        int currentSize = 0;
        while (currentSize < requestSize) {
            DataBatch elem = batches.remove(0);
            retBatches.add(elem);
            currentSize += elem.body.length;
            totalSize -= elem.body.length;
        }
        return retBatches;
    }
}

