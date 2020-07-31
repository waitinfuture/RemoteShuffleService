package com.aliyun.emr.ess.service.deploy.worker;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class FileWriter {
    private static final Logger logger = LoggerFactory.getLogger(FileWriter.class);

    private final File file;
    private final FileChannel channel;
    private volatile boolean closed;

    private final AtomicInteger numPendingWrites = new AtomicInteger();
    private final ArrayList<Long> chunkOffsets = new ArrayList<>();
    private long nextBoundary;
    private long bytesWritten;

    private final DiskFlusher flusher;
    private ByteBuffer flushBuffer;

    private final long chunkSize;

    static class FlushNotifier {
        final AtomicInteger numPendingFlushes = new AtomicInteger();
        final AtomicReference<IOException> exception = new AtomicReference<>();

        void setException(IOException e) {
            exception.set(e);
        }

        boolean hasException() {
            return exception.get() != null;
        }

        void checkException() throws IOException {
            IOException e = exception.get();
            if (e != null) {
                throw e;
            }
        }
    }

    private final FlushNotifier notifier = new FlushNotifier();

    public FileWriter(File file, DiskFlusher flusher, long chunkSize) throws FileNotFoundException {
        this.file = file;
        this.flusher = flusher;
        this.chunkSize = chunkSize;
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

    private void flush() throws IOException {
        notifier.checkException();
        flushBuffer.flip();
        notifier.numPendingFlushes.incrementAndGet();
        WriteTask task = new WriteTask(flushBuffer, channel, notifier);
        flusher.addTask(task);
    }

    /**
     * assume data size is less than chunk capacity
     *
     * @param data
     */
    public void write(ByteBuf data) throws IOException {
        if (closed) {
            String msg = "already closed!";
            logger.error(msg);
            throw new IOException(msg);
        }

        if (notifier.hasException()) {
            return;
        }

        synchronized (this) {
            if (bytesWritten >= nextBoundary) {
                chunkOffsets.add(bytesWritten);
                nextBoundary = bytesWritten + chunkSize;
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
        }
    }

    public long close() throws IOException {
        if (closed) {
            String msg = "already closed!";
            logger.error(msg);
            throw new IOException(msg);
        }
        closed = true;

        try {
            while (numPendingWrites.get() > 0) {
                try {
                    notifier.checkException();
                    TimeUnit.MILLISECONDS.sleep(20);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }

            if (flushBuffer.position() > 0) {
                flush();
                flushBuffer = null;
            }

            while (notifier.numPendingFlushes.get() > 0) {
                try {
                    notifier.checkException();
                    TimeUnit.MILLISECONDS.sleep(20);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        } finally {
            if (flushBuffer != null) {
                flusher.returnBuffer(flushBuffer);
                flushBuffer = null;
            }
            channel.close();
        }

        return bytesWritten;
    }

    public void destroy() throws IOException {
//        channel.close();
    }
}
