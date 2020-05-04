package com.aliyun.emr.jss.protocol;

import java.io.Serializable;
import java.net.URI;

public final class PartitionLocation implements Serializable
{
    private String applicationId;
    private int shuffleId;
    private int reduceId;
    private URI jssWorkerUrl;
    private boolean isActive;

    public PartitionLocation(String applicationId, int shuffleId, int reduceId, URI jssWorkerUrl) {
        this.applicationId = applicationId;
        this.shuffleId = shuffleId;
        this.reduceId = reduceId;
        this.jssWorkerUrl = jssWorkerUrl;
        this.isActive = false;
    }

    public PartitionLocation(String applicationId, int shuffleId, int reduceId, URI jssWorkerUrl, boolean isActive) {
        this.applicationId = applicationId;
        this.shuffleId = shuffleId;
        this.reduceId = reduceId;
        this.jssWorkerUrl = jssWorkerUrl;
        this.isActive = isActive;
    }

    public String getApplicationId()
    {
        return applicationId;
    }

    public int getShuffleId()
    {
        return shuffleId;
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
