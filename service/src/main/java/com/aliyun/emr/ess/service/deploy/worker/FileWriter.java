package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.ess.unsafe.Platform;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public final class FileWriter {
    private static final Logger logger = LoggerFactory.getLogger(FileWriter.class);

    private static final int CHUNK_SIZE = 1024 * 1024 * 8;

    private final File file;
    private final FileChannel channel;
    private volatile boolean closed;

    private final AtomicInteger numPendingWrites = new AtomicInteger();
    private final ArrayList<Long> chunkOffsets = new ArrayList<>();
    private long nextBoundary;
    private long bytesWritten;

    private final DiskFlusher flusher;
    private ByteBuffer flushBuffer;
    private final AtomicInteger numPendingFlushes = new AtomicInteger();

    public FileWriter(File file, DiskFlusher flusher) throws FileNotFoundException {
        this.file = file;
        this.flusher = flusher;
        channel = new FileOutputStream(file).getChannel();
        flushBuffer = flusher.takeBuffer();
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

    private void flush() {
        flushBuffer.flip();
        numPendingFlushes.incrementAndGet();
        WriteTask task = new WriteTask(flushBuffer, channel, numPendingFlushes);
        flusher.addTask(task);
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

            final int numBytes = data.readableBytes();

            if (flushBuffer.position() + numBytes >= flushBuffer.capacity()) {
                flush();
                flushBuffer = flusher.takeBuffer();
            }

            flushBuffer.limit(flushBuffer.position() + numBytes);
            data.getBytes(data.readerIndex(), flushBuffer);

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

        if (flushBuffer.position() > 0) {
            flush();
            flushBuffer = null;
        }

        while (numPendingFlushes.get() > 0) {
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
//        channel.close();
    }
}
