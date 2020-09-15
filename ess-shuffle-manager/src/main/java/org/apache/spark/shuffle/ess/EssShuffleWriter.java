package org.apache.spark.shuffle.ess;

import com.aliyun.emr.ess.client.ShuffleClient;
import com.aliyun.emr.ess.common.EssConf;

import com.aliyun.emr.ess.common.util.Utils;
import org.apache.spark.*;
import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.BaseShuffleHandle;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.PartitionIdPassthrough;
import org.apache.spark.sql.execution.UnsafeRowSerializer;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.BlockManagerId$;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Product2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Time;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Private
public class EssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(EssShuffleWriter.class);

    private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

    private static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

    private final int SEND_BUFFER_SIZE;
    private final ShuffleDependency<K, V, C> dep;
    private final SerializerInstance serializer;
    private final Partitioner partitioner;
    private final ShuffleWriteMetrics writeMetrics;
    private final String appId;
    private final int shuffleId;
    private final int mapId;
    private final TaskContext taskContext;
    private final SparkConf sparkConf;
    private final ShuffleClient essShuffleClient;
    private final int numMappers;
    private final int numPartitions;

    @Nullable
    private MapStatus mapStatus;
    private long peakMemoryUsedBytes = 0;

    /**
     * Subclass of ByteArrayOutputStream that exposes `buf` directly.
     */
    private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
        MyByteArrayOutputStream(int size) {
            super(size);
        }

        public byte[] getBuf() {
            return buf;
        }
    }

    private final MyByteArrayOutputStream serBuffer;
    private final SerializationStream serOutputStream;

    private byte[][] sendBuffers;
    private int[] sendOffsets;

    private final long[] mapStatusLengths;
    private final long[] mapStatusRecords;
    private final long[] tmpLengths;
    private final long[] tmpRecords;

    /**
     * Are we in the process of stopping? Because map tasks can call stop() with success = true
     * and then call stop() with success = false if they get an exception, we want to make sure
     * we don't try deleting files, etc twice.
     */
    private volatile boolean stopping = false;

    private class PushTask {
        int partitionId;
        final byte[] buffer = new byte[SEND_BUFFER_SIZE];
        int size;
    }

    private class DataPusher {
        private final long WAIT_TIME_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

        private final LinkedBlockingQueue<PushTask> idleQueue;
        private final LinkedBlockingQueue<PushTask> workingQueue;

        private final ReentrantLock idleLock = new ReentrantLock();
        private final Condition idleFull = idleLock.newCondition();

        private final AtomicReference<IOException> exception = new AtomicReference<>();

        private volatile boolean terminated;

        final Thread worker = new Thread("DataPusher-" + taskContext.taskAttemptId()) {
            private void reclaimTask(PushTask task) throws InterruptedException {
                idleLock.lockInterruptibly();
                try {
                    idleQueue.put(task);
                    if (idleQueue.remainingCapacity() == 0) {
                        idleFull.signal();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    exception.set(new IOException(e));
                } finally {
                    idleLock.unlock();
                }
            }

            @Override
            public void run() {
                while (!terminated && !stopping && exception.get() == null) {
                    try {
                        PushTask task = workingQueue.poll(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
                        if (task == null) {
                            continue;
                        }
                        pushData(task.partitionId, task.buffer, task.size);
                        reclaimTask(task);
                    } catch (InterruptedException e) {
                        exception.set(new IOException(e));
                    } catch (IOException e) {
                        exception.set(e);
                    }
                }
            }
        };

        DataPusher(int capacity) throws IOException {
            idleQueue = new LinkedBlockingQueue<>(capacity);
            workingQueue = new LinkedBlockingQueue<>(capacity);

            for (int i = 0; i < capacity; i++) {
                try {
                    idleQueue.put(new PushTask());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            worker.start();
        }

        void addTask(int partitionId, byte[] buffer, int size) throws IOException {
            try {
                PushTask task = null;
                while (task == null) {
                    checkException();
                    task = idleQueue.poll(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
                }
                task.partitionId = partitionId;
                System.arraycopy(buffer, 0, task.buffer, 0, size);
                task.size = size;
                while (!workingQueue.offer(task, WAIT_TIME_NANOS, TimeUnit.NANOSECONDS)) {
                    checkException();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                IOException ioe = new IOException(e);
                exception.set(ioe);
                throw ioe;
            }
        }

        void waitOnTermination() throws IOException {
            try {
                idleLock.lockInterruptibly();
                waitIdleQueueFullWithLock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exception.set(new IOException(e));
            }

            terminated = true;
            idleQueue.clear();
            workingQueue.clear();
            checkException();
        }

        private void checkException() throws IOException {
            if (exception.get() != null) {
                throw exception.get();
            }
        }

        private void pushData(int partitionId, byte[] buffer, int size) throws IOException {
            int bytesWritten = essShuffleClient.pushData(
                appId,
                shuffleId,
                mapId,
                taskContext.attemptNumber(),
                partitionId,
                buffer,
                0,
                size,
                numMappers,
                numPartitions
            );
            writeMetrics.incBytesWritten(bytesWritten);
        }

        private void waitIdleQueueFullWithLock() {
            try {
                while (idleQueue.remainingCapacity() > 0 && exception.get() == null) {
                    idleFull.await(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exception.set(new IOException(e));
            } finally {
                idleLock.unlock();
            }
        }
    }

    private final DataPusher dataPusher;

    public EssShuffleWriter(
        BaseShuffleHandle<K, V, C> handle,
        int mapId,
        TaskContext taskContext,
        SparkConf sparkConf,
        int numMappers,
        int numPartitions) throws IOException {
        this.mapId = mapId;
        this.dep = handle.dependency();
        this.appId = sparkConf.getAppId();
        this.shuffleId = dep.shuffleId();
        this.serializer = dep.serializer().newInstance();
        this.partitioner = dep.partitioner();
        this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
        this.taskContext = taskContext;
        this.sparkConf = sparkConf;
        this.numMappers = numMappers;
        this.numPartitions = numPartitions;
        EssConf conf = EssShuffleManager.fromSparkConf(this.sparkConf);
        this.essShuffleClient = ShuffleClient.get(conf);

        serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
        serOutputStream = serializer.serializeStream(serBuffer);

        mapStatusLengths = new long[partitioner.numPartitions()];
        mapStatusRecords = new long[partitioner.numPartitions()];
        tmpLengths = new long[partitioner.numPartitions()];
        tmpRecords = new long[partitioner.numPartitions()];

        SEND_BUFFER_SIZE = (int) EssConf.essPushDataBufferSize(conf);

        sendBuffers = new byte[partitioner.numPartitions()][];
        sendOffsets = new int[partitioner.numPartitions()];

        dataPusher = new DataPusher(EssConf.essPushDataQueueCapacity(conf));
    }

    @Override
    public void write(scala.collection.Iterator<Product2<K, V>> records) throws IOException {
        if (canUseFastWrite()) {
            fastWrite0(records);
        } else if (dep.mapSideCombine()) {
            if (dep.aggregator().isEmpty()) {
                throw new UnsupportedOperationException("map side combine");
            }
            write0(dep.aggregator().get().combineValuesByKey(records, taskContext));
        } else {
            write0(records);
        }
        close();
    }

    private boolean canUseFastWrite() {
        return dep.serializer() instanceof UnsafeRowSerializer &&
            partitioner instanceof PartitionIdPassthrough;
    }

    private void fastWrite0(scala.collection.Iterator iterator) throws IOException {
        final scala.collection.Iterator<Product2<Integer, UnsafeRow>> records = iterator;

        while (records.hasNext()) {
            final Product2<Integer, UnsafeRow> record = records.next();
            final int partitionId = record._1();
            final UnsafeRow row = record._2();

            final int rowSize = row.getSizeInBytes();
            final int serializedRecordSize = 4 + rowSize;

            byte[] buffer = sendBuffers[partitionId];
            if (buffer == null) {
                buffer = new byte[SEND_BUFFER_SIZE];
                sendBuffers[partitionId] = buffer;
                peakMemoryUsedBytes += SEND_BUFFER_SIZE;
            }

            if (serializedRecordSize > SEND_BUFFER_SIZE) {
                pushGiantRecord(partitionId);
            } else {
                int offset = sendOffsets[partitionId];
                if ((SEND_BUFFER_SIZE - offset) < serializedRecordSize) {
                    flushSendBuffer(partitionId, buffer, offset);
                    updateMapStatus();
                    offset = 0;
                }

                Platform.putInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset, Integer.reverseBytes(rowSize));
                Platform.copyMemory(row.getBaseObject(), row.getBaseOffset(),
                    buffer, Platform.BYTE_ARRAY_OFFSET + offset + 4, rowSize);
                sendOffsets[partitionId] = offset + serializedRecordSize;
            }
            tmpLengths[partitionId] += serializedRecordSize;
            tmpRecords[partitionId] += 1;
        }
    }

    private void write0(scala.collection.Iterator iterator) throws IOException {
        final scala.collection.Iterator<Product2<K, ?>> records = iterator;

        while (records.hasNext()) {
            final Product2<K, ?> record = records.next();
            final K key = record._1();
            final int partitionId = partitioner.getPartition(key);
            serBuffer.reset();
            serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
            serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
            serOutputStream.flush();

            final int serializedRecordSize = serBuffer.size();
            assert (serializedRecordSize > 0);

            byte[] buffer = sendBuffers[partitionId];
            if (buffer == null) {
                buffer = new byte[SEND_BUFFER_SIZE];
                sendBuffers[partitionId] = buffer;
                peakMemoryUsedBytes += SEND_BUFFER_SIZE;
            }

            if (serializedRecordSize > SEND_BUFFER_SIZE) {
                pushGiantRecord(partitionId);
            } else {
                int offset = sendOffsets[partitionId];
                if ((SEND_BUFFER_SIZE - offset) < serializedRecordSize) {
                    flushSendBuffer(partitionId, buffer, offset);
                    updateMapStatus();
                    offset = 0;
                }
                System.arraycopy(serBuffer.getBuf(), 0, buffer, offset, serializedRecordSize);
                sendOffsets[partitionId] = offset + serializedRecordSize;
            }
            tmpLengths[partitionId] += serializedRecordSize;
            tmpRecords[partitionId] += 1;
        }
    }

    private void pushGiantRecord(int partitionId) throws IOException {
        int numBytes = serBuffer.size();
        logger.info("push giant record, size " + numBytes);
        long pushStartTime = System.nanoTime();
        essShuffleClient.pushData(
            appId,
            shuffleId,
            mapId,
            taskContext.attemptNumber(),
            partitionId,
            serBuffer.getBuf(),
            0,
            numBytes,
            numMappers,
            numPartitions
        );
        writeMetrics.incBytesWritten(serBuffer.size());
        writeMetrics.incWriteTime(System.nanoTime() - pushStartTime);
    }

    private void flushSendBuffer(int partitionId, byte[] buffer, int size) throws IOException {
        long pushStartTime = System.nanoTime();
        dataPusher.addTask(partitionId, buffer, size);
        writeMetrics.incWriteTime(System.nanoTime() - pushStartTime);
    }

    private void close() throws IOException {
        // merge and push residual data
        for (int i = 0; i < sendBuffers.length; i++) {
            final int size = sendOffsets[i];
            if (size > 0) {
                int bytesWritten = essShuffleClient.mergeData(
                    appId,
                    shuffleId,
                    mapId,
                    taskContext.attemptNumber(),
                    i,
                    sendBuffers[i],
                    0,
                    size,
                    numMappers,
                    numPartitions
                );
                // free buffer
                sendBuffers[i] = null;
                writeMetrics.incBytesWritten(bytesWritten);
            }
        }
        essShuffleClient.pushMergedData(appId, shuffleId, mapId, taskContext.attemptNumber());

        dataPusher.waitOnTermination();
        updateMapStatus();

        sendBuffers = null;
        sendOffsets = null;

        long waitStartTime = System.nanoTime();
        essShuffleClient.mapperEnd(appId, shuffleId, mapId, taskContext
            .attemptNumber(), numMappers);
        writeMetrics.incWriteTime(System.nanoTime() - waitStartTime);

        BlockManagerId dummyId = BlockManagerId$.MODULE$.apply(
            "rss", "127.0.0.1", 1111, Option.apply(null));
        mapStatus = SparkUtils.createMapStatus(dummyId, mapStatusLengths, mapStatusRecords);
    }

    private void updateMapStatus() {
        long recordsWritten = 0;
        for (int i = 0; i < partitioner.numPartitions(); i++) {
            mapStatusLengths[i] += tmpLengths[i];
            tmpLengths[i] = 0;
            mapStatusRecords[i] += tmpRecords[i];
            recordsWritten += tmpRecords[i];
            tmpRecords[i] = 0;
        }
        writeMetrics.incRecordsWritten(recordsWritten);
    }

    @Override
    public Option<MapStatus> stop(boolean success) {
        try {
            taskContext.taskMetrics().incPeakExecutionMemory(peakMemoryUsedBytes);

            if (stopping) {
                return Option.apply(null);
            } else {
                stopping = true;
                if (success) {
                    if (mapStatus == null) {
                        throw new IllegalStateException("Cannot call stop(true) without having called write()");
                    }
                    logger.info("mapStatus " + mapStatus.getSizeForBlock(0) + ", " + mapStatus.getRecordForBlock(0));
                    return Option.apply(mapStatus);
                } else {
                    return Option.apply(null);
                }
            }
        } finally {
            essShuffleClient.cleanup(appId, shuffleId, mapId, taskContext.attemptNumber());
        }
    }
}

