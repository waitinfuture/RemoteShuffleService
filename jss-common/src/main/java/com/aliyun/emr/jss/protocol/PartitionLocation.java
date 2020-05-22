package com.aliyun.emr.jss.protocol;

import java.io.Serializable;

public class PartitionLocation implements Serializable
{
    public enum Mode {
        Master, Slave
    };

    private String UUID;
    private String host;
    private int port;
    private Mode mode;
    private PartitionLocation peer;

    public PartitionLocation(PartitionLocation loc) {
        this.UUID = loc.UUID;
        this.host = loc.host;
        this.port = loc.port;
        this.mode = loc.mode;
        this.peer = loc.peer;
    }

    public PartitionLocation(String UUID, String host, int port, Mode mode) {
        this.UUID = UUID;
        this.host = host;
        this.port = port;
        this.mode = mode;
    }

    public PartitionLocation(String UUID, String host, int port, Mode mode, PartitionLocation peer) {
        this.UUID = UUID;
        this.host = host;
        this.port = port;
        this.mode = mode;
        this.peer = peer;
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

    public PartitionLocation getPeer() {
        return peer;
    }

    public void setPeer(PartitionLocation peer) {
        this.peer = peer;
    }

    public boolean equals(Object other) {
        if (!(other instanceof PartitionLocation)) {
            return false;
        }
        PartitionLocation o = (PartitionLocation) other;
        return host == o.host && port == o.port && UUID == o.UUID;
    }

    public int hashCode() {
        return (host + port + UUID).hashCode();
    }

    public String toString() {
        return UUID + " " + host + ":" + port + " " + "Mode: " + mode;
    }
}
