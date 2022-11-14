package org.apache.tez.shuffle.ess;

import com.aliyun.emr.ess.client.ShuffleClient;
import com.aliyun.emr.ess.common.EssConf;
import com.aliyun.emr.ess.unsafe.Platform;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag$;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class EssShuffleWriter {
    private static final Logger logger = LoggerFactory.getLogger(EssShuffleWriter.class);

    private static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

    private final int taskAttemptId;
    private final int SEND_BUFFER_SIZE;
    private final SerializerInstance serializer;
    private final String appId;
    private final int shuffleId;
    private final long mapId;
    private final ShuffleClient essShuffleClient;
    private final int numMappers;
    private final int numPartitions;
    private final Class keyClass;
    private final Class valClass;
    private final EssConf essConf;

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

        final Thread worker = new Thread("DataPusher-" + taskAttemptId) {
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
                (int) mapId,
                taskAttemptId,
                partitionId,
                buffer,
                0,
                size,
                numMappers,
                numPartitions
            );
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
        String appId,
        int shuffleId,
        long mapId,
        int taskAttemptId,
        int numMappers,
        int numPartitions,
        Class keyClass,
        Class valClass,
        EssConf essConf) throws IOException {
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.kryo.classesToRegister", "org.apache.hadoop.hive.ql.io.HiveKey,org.apache.hadoop.io.BytesWritable");
        this.serializer = new KryoSerializer(sparkConf).newInstance();
        this.taskAttemptId = taskAttemptId;
        this.numMappers = numMappers;
        this.numPartitions = numPartitions;
        this.keyClass = keyClass;
        this.valClass = valClass;
        this.essConf = essConf;
        this.essShuffleClient = ShuffleClient.get(essConf);

        serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
        serOutputStream = serializer.serializeStream(serBuffer);

        SEND_BUFFER_SIZE = (int) EssConf.essPushDataBufferSize(essConf);

        sendBuffers = new byte[numPartitions][];
        sendOffsets = new int[numPartitions];

        dataPusher = new DataPusher(EssConf.essPushDataQueueCapacity(essConf));
    }

    public int write(Object key, Object value, int partitionId) throws IOException {
        serBuffer.reset();
        serOutputStream.writeKey(key, ClassTag$.MODULE$.apply(keyClass));
        serOutputStream.writeValue(value, ClassTag$.MODULE$.apply(valClass));
        serOutputStream.flush();
        final int serializedRecordSize = serBuffer.size();
        assert (serializedRecordSize > 0);

        byte[] buffer = sendBuffers[partitionId];
        if (buffer == null) {
            buffer = new byte[SEND_BUFFER_SIZE];
            sendBuffers[partitionId] = buffer;
        }

        if (serializedRecordSize > SEND_BUFFER_SIZE) {
            // TODO push giant record
            pushGiantRecord(partitionId);
        } else {
            int offset = sendOffsets[partitionId];
            if ((SEND_BUFFER_SIZE - offset) < serializedRecordSize) {
                flushSendBuffer(partitionId, buffer, offset);
                offset = 0;
            }
            System.arraycopy(serBuffer.getBuf(), 0, buffer, offset, serializedRecordSize);
            sendOffsets[partitionId] = offset + serializedRecordSize;
        }
        return serializedRecordSize;
    }

    public int write(byte[] data, int keyLength, int valLength, int partitionId) throws IOException {
        final int serializedRecordSize = keyLength + valLength + 8;
        assert (serializedRecordSize > 0);

        byte[] buffer = sendBuffers[partitionId];
        if (buffer == null) {
            buffer = new byte[SEND_BUFFER_SIZE];
            sendBuffers[partitionId] = buffer;
        }

        if (serializedRecordSize > SEND_BUFFER_SIZE) {
            // TODO push giant record
            pushGiantRecord(partitionId);
        } else {
            int offset = sendOffsets[partitionId];
            if ((SEND_BUFFER_SIZE - offset) < serializedRecordSize) {
                flushSendBuffer(partitionId, buffer, offset);
                offset = 0;
            }
            // keylen,vallen,keydata,valdata
            Platform.putInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset, keyLength);
            Platform.putInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset + 4, valLength);
            System.arraycopy(data, 0, buffer, offset + 8, keyLength);
            System.arraycopy(data, keyLength, buffer, offset + 8 + keyLength, valLength);
            sendOffsets[partitionId] = offset + serializedRecordSize;
        }
        return serializedRecordSize;
    }

    private void pushGiantRecord(int partitionId) throws IOException {
        int numBytes = serBuffer.size();
        logger.info("push giant record, size " + numBytes);
        essShuffleClient.pushData(
            appId,
            shuffleId,
            (int) mapId,
            partitionId,
            taskAttemptId,
            serBuffer.getBuf(),
            0,
            numBytes,
            numMappers,
            numPartitions
        );
    }

    private void flushSendBuffer(int partitionId, byte[] buffer, int size) throws IOException {
        long pushStartTime = System.nanoTime();
        dataPusher.addTask(partitionId, buffer, size);
    }

    public void close() throws IOException {
        // here we wait for all the in-flight batches to return which sent by dataPusher thread
        dataPusher.waitOnTermination();
        essShuffleClient.prepareForMergeData(shuffleId, (int) mapId, taskAttemptId);

        // merge and push residual data to reduce network traffic
        // NB: since dataPusher thread have no in-flight data at this point,
        //     we now push merged data by task thread will not introduce any contention
        for (int i = 0; i < sendBuffers.length; i++) {
            final int size = sendOffsets[i];
            if (size > 0) {
                int bytesWritten = essShuffleClient.mergeData(
                    appId,
                    shuffleId,
                    (int) mapId,
                    taskAttemptId,
                    i,
                    sendBuffers[i],
                    0,
                    size,
                    numMappers,
                    numPartitions
                );
                // free buffer
                sendBuffers[i] = null;
            }
        }
        essShuffleClient.pushMergedData(appId, shuffleId, (int) mapId, taskAttemptId);

        sendBuffers = null;
        sendOffsets = null;

        essShuffleClient.mapperEnd(appId, shuffleId, (int) mapId, taskAttemptId, numMappers);
    }
}

