package com.aliyun.emr.ess.service.deploy.worker;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class FileWriter {
    private static final Logger logger = LoggerFactory.getLogger(FileWriter.class);

    private static long CHUNK_SIZE = 1024L * 1024L * 8;

    private final File file;
    private final FileChannel channel;
    private volatile boolean closed;

    private final AtomicInteger numPendingWrites = new AtomicInteger();
    private final ArrayList<Long> chunkOffsets = new ArrayList<>();
    private long nextBoundary;
    private long bytesWritten;

    public FileWriter(File file) throws FileNotFoundException {
        this.file = file;
        channel = new FileOutputStream(file).getChannel();
    }

    public File getFile() {
        return file;
    }

    public ArrayList<Long> getChunkOffsets() {
        return chunkOffsets;
    }

    public long getFileLength() {
        return bytesWritten;
    }

    public void incrementPendingWrites() {
        numPendingWrites.incrementAndGet();
    }

    /**
     * assume data size is less than chunk capacity
     *
     * @param data
     * @return written bytes
     */
    public int write(ByteBuf data) throws IOException {
        if (closed) {
            String msg = "already closed!";
            logger.error(msg);
            throw new IOException(msg);
        }
        synchronized (this) {
            if (bytesWritten >= nextBoundary) {
                chunkOffsets.add(bytesWritten);
                nextBoundary = bytesWritten + CHUNK_SIZE;
            }
            final int numBytes = channel.write(data.nioBuffer());
            bytesWritten += numBytes;
            numPendingWrites.decrementAndGet();
            return numBytes;
        }
    }

    public long close() throws IOException {
        if (closed) {
            String msg = "already closed!";
            logger.error(msg);
            throw new IOException(msg);
        }

        while (numPendingWrites.get() > 0) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        closed = true;
        channel.close();
        return bytesWritten;
    }

    public void destroy() throws IOException {
        channel.close();
    }
}
