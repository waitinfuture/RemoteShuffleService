package com.aliyun.emr.jss.service.deploy.worker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class MemoryPoolSuiteJ {
    @Test
    public void testAppend() {
        MemoryPool memoryPool = new MemoryPool(1024, 128);
        Chunk ch1 = memoryPool.allocateChunk();
        Chunk ch2 = memoryPool.allocateChunk();
        try {
            File file = new File("tmp");
            if (file.exists()) {
                file.delete();
            }
            DoubleChunk doubleChunk = new DoubleChunk(ch1, ch2, memoryPool, "tmp");
            byte[] bytes = new byte[64];
            ByteBuf data = Unpooled.copiedBuffer(bytes);
            doubleChunk.append(data);
            data.resetReaderIndex();
            doubleChunk.append(data);
            data.resetReaderIndex();
            doubleChunk.append(data);
            data.resetReaderIndex();
            doubleChunk.append(data);
            data.resetReaderIndex();
            Thread.sleep(100);
            System.out.println(file.length());
            assert file.length() == 128;
            doubleChunk.flush();
            Thread.sleep(100);
            assert file.length() == 256;
            doubleChunk.append(data);
            data.resetReaderIndex();
            Thread.sleep(100);
            assert file.length() == 256;
            doubleChunk.flush();
            Thread.sleep(100);
            assert file.length() == 320;
        } catch (Exception e) {
            // just ignore
        }
    }
}
