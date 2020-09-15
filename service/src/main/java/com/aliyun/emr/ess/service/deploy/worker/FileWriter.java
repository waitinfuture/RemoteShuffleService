package com.aliyun.emr.ess.service.deploy.worker;

import com.aliyun.emr.ess.common.exception.AlreadyClosedException;
import com.aliyun.emr.ess.common.metrics.source.AbstractSource;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.runtime.AbstractFunction0;

import java.io.File;
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

    private static final long WAIT_INTERVAL_MS = 20;

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
    private final long timeoutMs;

    private final AbstractSource workerSource;

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

    public FileWriter(
        File file, DiskFlusher flusher, long chunkSize, long timeoutMs,
        AbstractSource workerSource) throws IOException {
        this.file = file;
        this.flusher = flusher;
        this.chunkSize = chunkSize;
        this.timeoutMs = timeoutMs;
        this.workerSource = workerSource;
        channel = new FileOutputStream(file).getChannel();
        AbstractFunction0<IOException> takeBufferFunc = new AbstractFunction0<IOException>() {
            @Override
            public IOException apply() {
                try {
                    takeBuffer();
                } catch (IOException e) {
                    return e;
                }
                return null;
            }
        };
        IOException e = workerSource.sample(WorkerSource.FileWriterInitTakeBufferTime(),
            file.getAbsolutePath(), takeBufferFunc);
        if (e != null) {
            throw e;
        }
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

    public void decrementPendingWrites() {
        numPendingWrites.decrementAndGet();
    }

    private void flush() throws IOException {
        notifier.checkException();
        flushBuffer.flip();
        notifier.numPendingFlushes.incrementAndGet();
        FlushTask task = new FlushTask(flushBuffer, channel, notifier);
        addTask(task);
        flushBuffer = null;
    }

    private void flush(ByteBuffer giantBatch) throws IOException {
        notifier.checkException();
        notifier.numPendingFlushes.incrementAndGet();
        FlushTask task = new FlushTask(giantBatch, channel, notifier);
        addTask(task);
    }

    /**
     * assume data size is less than chunk capacity
     *
     * @param data
     */
    public void write(ByteBuf data) throws IOException {
        if (closed) {
            String msg = "[write] already closed!, fileName " + file.getAbsolutePath();
            logger.warn(msg);
            throw new AlreadyClosedException(msg);
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

            if (flushBuffer.capacity() < numBytes) {
                flush(data.nioBuffer());
                logger.info("flush giant record, size " + numBytes);
            } else {
                if (flushBuffer.position() + numBytes >= flushBuffer.capacity()) {
                    flush();
                    takeBuffer();
                }

                flushBuffer.limit(flushBuffer.position() + numBytes);
                data.getBytes(data.readerIndex(), flushBuffer);
            }

            bytesWritten += numBytes;
            numPendingWrites.decrementAndGet();
        }
    }

    public long close() throws IOException {
        if (closed) {
            String msg = "[close] already closed! fileName " + file.getAbsolutePath();
            logger.error(msg);
            throw new AlreadyClosedException(msg);
        }

        try {
            waitOnNoPending(numPendingWrites);
            closed = true;

            synchronized (this) {
                if (flushBuffer.position() > 0) {
                    flush();
                }
            }

            waitOnNoPending(notifier.numPendingFlushes);
        } finally {
            returnBuffer();
            channel.close();
        }

        return bytesWritten;
    }

    public void destroy() {
        if (!closed) {
            closed = true;
            notifier.setException(new IOException("destroyed"));
            returnBuffer();
            try {
                channel.close();
            } catch (IOException e) {
                logger.warn("close channel failed: " + file);
            }
        }
        file.delete();
    }

    private void waitOnNoPending(AtomicInteger counter) throws IOException {
        long waitTime = timeoutMs;
        while (counter.get() > 0 && waitTime > 0) {
            try {
                notifier.checkException();
                TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS);
            } catch (InterruptedException e) {
                IOException ioe = new IOException(e);
                notifier.setException(ioe);
                throw ioe;
            }
            waitTime -= WAIT_INTERVAL_MS;
        }
        if (counter.get() > 0) {
            IOException ioe = new IOException("wait pending actions timeout");
            notifier.setException(ioe);
            throw ioe;
        }
        notifier.checkException();
    }

    private void takeBuffer() throws IOException {
        flushBuffer = flusher.takeBuffer(timeoutMs);
        if (flushBuffer == null) {
            IOException e = new IOException("take buffer timeout");
            notifier.setException(e);
            throw e;
        }
    }

    private void addTask(FlushTask task) throws IOException {
        if (!flusher.addTask(task, timeoutMs)) {
            IOException e = new IOException("add flush task timeout");
            notifier.setException(e);
            throw e;
        }
    }

    private synchronized void returnBuffer() {
        if (flushBuffer != null) {
            flusher.returnBuffer(flushBuffer);
            flushBuffer = null;
        }
    }
}
