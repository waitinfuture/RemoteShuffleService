package org.apache.spark.shuffle.ess;

import com.aliyun.emr.jss.client.ShuffleClient;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.TaskContext;
import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
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
import org.apache.spark.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Product2;
import scala.concurrent.Future;
import scala.concurrent.Promise$;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.BoxedUnit;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Private
public class EssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(EssShuffleWriter.class);

    private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

    private static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;
    private static final int SEND_BUFFER_SIZE = 128 * 1024;

    private final TaskMemoryManager memoryManager;
    private final ShuffleDependency<K, V, C> dep;
    private final SerializerInstance serializer;
    private final Partitioner partitioner;
    private final ShuffleWriteMetrics writeMetrics;
    private final int shuffleId;
    private final int mapId;
    private final TaskContext taskContext;
    private final SparkConf sparkConf;
    private final ShuffleClient essShuffleClient;
    private final ExecutorService executorService =
        ThreadUtils.newDaemonCachedThreadPool(
            "EssShuffleWriter-Client", 8 ,60);

    @Nullable private MapStatus mapStatus;
    private long peakMemoryUsedBytes = 0;

    private final List<Future<BoxedUnit>> futures = new LinkedList<>();

    /** Subclass of ByteArrayOutputStream that exposes `buf` directly. */
    private static final class MyByteArrayOutputStream extends ByteArrayOutputStream {
        MyByteArrayOutputStream(int size) { super(size); }
        public byte[] getBuf() { return buf; }
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
    private boolean stopping = false;

    public EssShuffleWriter(
        TaskMemoryManager memoryManager,
        BaseShuffleHandle<K, V, C> handle,
        int mapId,
        TaskContext taskContext,
        SparkConf sparkConf) {
        this.memoryManager = memoryManager;
        this.mapId = mapId;
        this.dep = handle.dependency();
        this.shuffleId = dep.shuffleId();
        this.serializer = dep.serializer().newInstance();
        this.partitioner = dep.partitioner();
        this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
        this.taskContext = taskContext;
        this.sparkConf = sparkConf;
        this.essShuffleClient = ShuffleClient.get(EssShuffleManager.fromSparkConf(this.sparkConf));
        this.essShuffleClient.applyShuffleInfo(
            sparkConf.getAppId(),
            shuffleId,
            ((EssShuffleHandle) handle).initPartitionLocations());

        serBuffer = new MyByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
        serOutputStream = serializer.serializeStream(serBuffer);

        sendBuffers = new byte[partitioner.numPartitions()][];
        sendOffsets = new int[partitioner.numPartitions()];
    }

    private void updatePeakMemoryUsed() {
    }

    /**
     * Return the peak memory used so far, in bytes.
     */
    public long getPeakMemoryUsedBytes() {
        updatePeakMemoryUsed();
        return peakMemoryUsedBytes;
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
            }

            int offset = sendOffsets[partitionId];
            if ((SEND_BUFFER_SIZE - offset) < serializedRecordSize) {
                byte[] tmpBuffer = new byte[SEND_BUFFER_SIZE];
                System.arraycopy(buffer, 0, tmpBuffer, 0, SEND_BUFFER_SIZE);
                flushSendBuffer(partitionId, tmpBuffer, offset);
                offset = 0;
            }

            Platform.putInt(buffer, Platform.BYTE_ARRAY_OFFSET + offset, Integer.reverseBytes(rowSize));
            Platform.copyMemory(row.getBaseObject(), row.getBaseOffset(),
                buffer, Platform.BYTE_ARRAY_OFFSET + offset + 4, rowSize);
            sendOffsets[partitionId] = offset + serializedRecordSize;
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
            }

            int offset = sendOffsets[partitionId];
            if ((SEND_BUFFER_SIZE - offset) < serializedRecordSize) {
                byte[] tmpBuffer = new byte[SEND_BUFFER_SIZE];
                System.arraycopy(buffer, 0, tmpBuffer, 0, SEND_BUFFER_SIZE);
                flushSendBuffer(partitionId, tmpBuffer, offset);
                offset = 0;
            }
            System.arraycopy(serBuffer.getBuf(), 0, buffer, offset, serializedRecordSize);
            sendOffsets[partitionId] = offset + serializedRecordSize;
        }
    }

    private void flushSendBuffer(int partitionId, byte[] buffer, int size) throws IOException {
        scala.concurrent.Promise<BoxedUnit> action = Promise$.MODULE$.apply();
        executorService.submit(new Runnable()
        {
            @Override
            public void run()
            {
                boolean result = essShuffleClient.pushData(
                    sparkConf.getAppId(),
                    shuffleId,
                    mapId,
                    taskContext.attemptNumber(),
                    partitionId,
                    buffer,
                    0,
                    size
                );

                if (result) {
                    action.success(BoxedUnit.UNIT);
                } else {
                    action.failure(new Exception("Push Data Failed."));
                }
            }
        });
        futures.add(action.future());
    }

    private void close() throws IOException {
        // flush
        for (int i = 0; i < sendBuffers.length; i++) {
            final int size = sendOffsets[i];
            if (size > 0) {
                flushSendBuffer(i, sendBuffers[i], size);
            }
        }
        sendBuffers = null;
        sendOffsets = null;

        for (Future<BoxedUnit> future : futures) {
            try {
                ThreadUtils.awaitReady(future, new FiniteDuration(30, TimeUnit.SECONDS));
            } catch (SparkException e) {
                throw new IOException("Failed to get future result.", e);
            }
        }
        futures.clear();

        essShuffleClient.mapperEnd(sparkConf.getAppId(), shuffleId, mapId, taskContext
            .attemptNumber());

        BlockManagerId dummyId = BlockManagerId$.MODULE$.apply(
            "amor", "127.0.0.1", 1111, Option.apply(null));
        mapStatus = MapStatus$.MODULE$.apply(
            dummyId, new long[partitioner.numPartitions()], new long[0]);
    }

    @Override
    public Option<MapStatus> stop(boolean success) {
        try {
            taskContext.taskMetrics().incPeakExecutionMemory(getPeakMemoryUsedBytes());

            if (stopping) {
                return Option.apply(null);
            } else {
                stopping = true;
                if (success) {
                    if (mapStatus == null) {
                        throw new IllegalStateException("Cannot call stop(true) without having called write()");
                    }
                    return Option.apply(mapStatus);
                } else {
                    return Option.apply(null);
                }
            }
        } finally {
            futures.clear();
        }
    }
}

