package com.aliyun.emr.ess.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;

public class EssHistogram extends Histogram {
    private final Reservoir reservoir;
    public EssHistogram(Reservoir reservoir) {
        super(reservoir);
        this.reservoir = reservoir;
    }

    public Reservoir getReservoir() {
        return reservoir;
    }
}
