package com.aliyun.emr.jss.service.deploy.worker;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

public class MemoryPoolSuiteJ
{
    @Test
    public void testAppend() throws Exception
    {
        MemoryPool memoryPool = new MemoryPool(1024, 128);
        Chunk ch1 = memoryPool.allocateChunk();
        Chunk ch2 = memoryPool.allocateChunk();
        Path file = new Path(
            String.format("%s/%s",
                "hdfs://11.158.199.162:9000/tmp/ess-test/",
                UUID.randomUUID().toString()
            )
        );
        Configuration hadoopConf = new Configuration();
        FileSystem fs = file.getFileSystem(hadoopConf);
        FileStatus status = null;

        if (fs.exists(file)) {
            fs.delete(file, true);
        }

        DoubleChunk doubleChunk = new DoubleChunk(ch1, ch2, memoryPool, file);
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
        Thread.sleep(1000);
        status = fs.getFileStatus(file);
        System.out.println(status.getLen());
        assert status.getLen() == 128;
        doubleChunk.append(data);
        data.resetReaderIndex();
        Thread.sleep(1000);
        status = fs.getFileStatus(file);
        assert status.getLen() == 256;
        doubleChunk.flush();
        Thread.sleep(1000);
        status = fs.getFileStatus(file);
        assert status.getLen() == 320;

    }
}

