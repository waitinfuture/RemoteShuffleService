package com.aliyun.emr.jss.protocol;

import java.io.Serializable;
import java.net.URI;

public final class PartitionLocation implements Serializable
{
    private int reduceId;
    private URI jssWorkerUrl;
    private boolean isActive;

    public PartitionLocation(int reduceId, URI jssWorkerUrl) {
        this.reduceId = reduceId;
        this.jssWorkerUrl = jssWorkerUrl;
        this.isActive = false;
    }

    public PartitionLocation(int reduceId, URI jssWorkerUrl, boolean isActive) {
        this.reduceId = reduceId;
        this.jssWorkerUrl = jssWorkerUrl;
        this.isActive = isActive;
    }

    public int getReduceId()
    {
        return reduceId;
    }

    public boolean isActive()
    {
        return isActive;
    }

    public void setActive(boolean active)
    {
        isActive = active;
    }

    public URI getJssWorkerUrl()
    {
        return jssWorkerUrl;
    }
}
