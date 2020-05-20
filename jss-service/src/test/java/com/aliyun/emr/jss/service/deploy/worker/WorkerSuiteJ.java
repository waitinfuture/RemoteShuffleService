package com.aliyun.emr.jss.service.deploy.worker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.junit.Test;

public class WorkerSuiteJ {
    @Test
    public void testPooledAllocator() {
        System.out.println(Runtime.getRuntime().maxMemory());
        System.out.println(PlatformDependent.maxDirectMemory());
        System.out.println(PlatformDependent.hasUnsafe());

        PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);
        ByteBuf buf = allocator.directBuffer(1024 * 1024 * 16);
        buf.release();
    }
}
