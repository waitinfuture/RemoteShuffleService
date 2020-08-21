package com.aliyun.emr.ess.metrics;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;

public class EssTimer extends Timer {
    private final Reservoir reservoir;

    public EssTimer(Reservoir reservoir) {
        super(reservoir);
        this.reservoir = reservoir;
    }

    public Reservoir getReservoir() {
        return reservoir;
    }
}
