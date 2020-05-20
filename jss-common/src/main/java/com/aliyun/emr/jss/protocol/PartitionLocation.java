package com.aliyun.emr.jss.protocol;

import java.io.Serializable;

public final class PartitionLocation implements Serializable
{
    public enum Mode {
        Master, Slave
    };

    private String UUID;
    private String host;
    private int port;
    private Mode mode;
    private PartitionLocation slavePartitionLocation;

    public PartitionLocation(String UUID, String host, int port, Mode mode) {
        this.UUID = UUID;
        this.host = host;
        this.port = port;
        this.mode = mode;
    }

    public PartitionLocation(String UUID, String host, int port, PartitionLocation slave) {
        this.UUID = UUID;
        this.host = host;
        this.port = port;
        this.mode = Mode.Master;
        this.slavePartitionLocation = slave;
    }

    public String getUUID() {
        return UUID;
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

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public PartitionLocation getSlavePartitionLocation() {
        return slavePartitionLocation;
    }

    public void setSlavePartitionLocation(PartitionLocation slavePartitionLocation) {
        this.slavePartitionLocation = slavePartitionLocation;
    }

    public String toString() {
        return UUID + "\n" + host + ":" + port + "\n" + "Mode: " + mode;
    }
}
