package com.aliyun.emr.ess.service.deploy.worker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class MemoryPoolSuiteJ
{
    @Test
    public void testAppend() throws Exception
    {
        MemoryPool memoryPool = new MemoryPool(1024, 128);

        File file = new File("tmp/ess-test", UUID.randomUUID().toString());

        FileWriter fileWriter = new FileWriter(file, null, 0);
        byte[] bytes = new byte[64];
        ByteBuf data = Unpooled.copiedBuffer(bytes);
        fileWriter.write(data);
        data.resetReaderIndex();
        fileWriter.write(data);
        data.resetReaderIndex();
        fileWriter.write(data);
        data.resetReaderIndex();
        fileWriter.write(data);
        data.resetReaderIndex();
        Thread.sleep(1000);

//        System.out.println(status.getLen());
//        assert status.getLen() == 128;
        fileWriter.write(data);
        data.resetReaderIndex();
        Thread.sleep(1000);

//        assert status.getLen() == 256;
        fileWriter.close();
        Thread.sleep(1000);

//        assert status.getLen() == 320;
    }
}

