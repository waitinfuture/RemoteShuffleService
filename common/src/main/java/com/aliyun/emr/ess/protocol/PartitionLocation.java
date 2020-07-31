package com.aliyun.emr.ess.protocol;

import java.io.Serializable;

public class PartitionLocation implements Serializable
{
    public enum Mode {
        Master(0), Slave(1);

        private final byte mode;

        Mode(int id) {
            assert id < 128 : "Cannot have more than 128 message types";
            this.mode = (byte) id;
        }

        public byte mode() { return mode; }
    };

    public static PartitionLocation.Mode getMode(byte mode) {
        if (mode == 0) {
            return Mode.Master;
        } else {
            return Mode.Slave;
        }
    }

    private int reduceId;
    private int epoch;
    private String host;
    private int port;
    private Mode mode;
    private PartitionLocation peer;

    public PartitionLocation(PartitionLocation loc) {
        this.reduceId = loc.reduceId;
        this.epoch = loc.epoch;
        this.host = loc.host;
        this.port = loc.port;
        this.mode = loc.mode;
        this.peer = loc.peer;
    }

    public PartitionLocation(int reduceId, int epoch, String host, int port, Mode mode) {
        this(reduceId, epoch, host, port, mode, null);
    }

    public PartitionLocation(int reduceId, int epoch, String host, int port, Mode mode, PartitionLocation peer) {
        this.reduceId = reduceId;
        this.epoch = epoch;
        this.host = host;
        this.port = port;
        this.mode = mode;
        this.peer = peer;
    }

    public int getReduceId()
    {
        return reduceId;
    }

    public void setReduceId(int reduceId)
    {
        this.reduceId = reduceId;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String hostPort() {
        return host + ":" + port;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public PartitionLocation getPeer() {
        return peer;
    }

    public void setPeer(PartitionLocation peer) {
        this.peer = peer;
    }

    public String getUniqueId() {
        return reduceId + "-" + epoch;
    }

    public String getFileName() {
        return reduceId + "-" + epoch + "-" + mode.mode;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PartitionLocation)) {
            return false;
        }
        PartitionLocation o = (PartitionLocation) other;
        return reduceId == o.reduceId && host.equals(o.host) && port == o.port && epoch == o.epoch;
    }

    @Override
    public int hashCode() {
        return (reduceId + host + port + epoch).hashCode();
    }

    @Override
    public String toString() {
        return reduceId + " " + epoch + " " + host + ":" + port + " " + "Mode: " + mode + (peer ==
            null);
    }
}
