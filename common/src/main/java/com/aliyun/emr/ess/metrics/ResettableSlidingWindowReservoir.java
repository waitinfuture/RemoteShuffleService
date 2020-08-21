package com.aliyun.emr.ess.metrics;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;

import java.util.Arrays;

import static java.lang.Math.min;

public class ResettableSlidingWindowReservoir implements Reservoir {
    private final long[] measurements;
    private long count;

    /**
     * Creates a new {@link ResettableSlidingWindowReservoir} which stores the last {@code size} measurements.
     *
     * @param size the number of measurements to store
     */
    public ResettableSlidingWindowReservoir(int size) {
        this.measurements = new long[size];
        this.count = 0;
    }

    @Override
    public synchronized int size() {
        return (int) min(count, measurements.length);
    }

    @Override
    public synchronized void update(long value) {
        measurements[(int) (count++ % measurements.length)] = value;
    }

    @Override
    public Snapshot getSnapshot() {
        final long[] values = new long[size()];
        for (int i = 0; i < values.length; i++) {
            synchronized (this) {
                values[i] = measurements[i];
            }
        }
        return new UniformSnapshot(values);
    }

    public synchronized void reset() {
        Arrays.fill(measurements, 0);
    }
}
