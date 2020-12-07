package com.aliyun.emr.ess.client.stream;

import com.aliyun.emr.ess.client.MetricsCallback;
import com.aliyun.emr.ess.client.compress.EssLz4CompressorTrait;
import com.aliyun.emr.ess.client.compress.EssLz4Decompressor;
import com.aliyun.emr.ess.common.EssConf;
import com.aliyun.emr.ess.protocol.PartitionLocation;
import com.aliyun.emr.ess.unsafe.Platform;
import com.aliyun.emr.network.buffer.ManagedBuffer;
import com.aliyun.emr.network.buffer.NettyManagedBuffer;
import com.aliyun.emr.network.client.ChunkReceivedCallback;
import com.aliyun.emr.network.client.TransportClient;
import com.aliyun.emr.network.client.TransportClientFactory;
import io.netty.buffer.ByteBuf;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


public abstract class EssInputStream extends InputStream {
    private static final Logger logger = Logger.getLogger(EssInputStream.class);

    public static EssInputStream create(
            EssConf conf,
            TransportClientFactory clientFactory,
            String shuffleKey,
            PartitionLocation[] locations,
            int[] attempts,
            int attemptNumber) throws IOException {
        if (locations == null || locations.length == 0) {
            return emptyInputStream;
        } else {
            return new EssInputStreamImpl(
                conf, clientFactory, shuffleKey, locations, attempts, attemptNumber);
        }
    }

    public static EssInputStream empty() {
        return emptyInputStream;
    }

    public abstract void setCallback(MetricsCallback callback);

    private static final EssInputStream emptyInputStream = new EssInputStream() {
        @Override
        public int read() throws IOException {
            return -1;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            return -1;
        }

        @Override
        public void setCallback(MetricsCallback callback) {
        }
    };

    private static final class EssInputStreamImpl extends EssInputStream {
        private final EssConf conf;
        private final TransportClientFactory clientFactory;
        private final String shuffleKey;
        private final PartitionLocation[] locations;
        private final int[] attempts;
        private final int attemptNumber;

        private final int maxInFlight;

        private final Map<Integer, Set<Integer>> batchesRead = new HashMap<>();

        private byte[] compressedBuf;
        private byte[] decompressedBuf;
        private final EssLz4Decompressor decompressor;

        private ByteBuf currentChunk;
        private PartitionReader currentReader;
        private int fileIndex;
        private int position;
        private int limit;

        private MetricsCallback callback;

        // mapId, attempId, batchId, size
        private final int BATCH_HEADER_SIZE = 4 * 4;
        private final byte[] sizeBuf = new byte[BATCH_HEADER_SIZE];

        public EssInputStreamImpl(
                EssConf conf,
                TransportClientFactory clientFactory,
                String shuffleKey,
                PartitionLocation[] locations,
                int[] attempts,
                int attemptNumber) throws IOException {
            this.conf = conf;
            this.clientFactory = clientFactory;
            this.shuffleKey = shuffleKey;
            this.locations = locations;
            this.attempts = attempts;
            this.attemptNumber = attemptNumber;

            maxInFlight = EssConf.essFetchChunkMaxReqsInFlight(conf);

            int blockSize = ((int) EssConf.essPushDataBufferSize(conf)) + EssLz4CompressorTrait
                    .HEADER_LENGTH;
            compressedBuf = new byte[blockSize];
            decompressedBuf = new byte[blockSize];

            decompressor = new EssLz4Decompressor();

            moveToNextReader();
        }

        private void moveToNextReader() throws IOException {
            logger.info("move to next partition " + locations[fileIndex]);
            if (currentReader != null) {
                currentReader.close();
            }
            currentReader = createReader(locations[fileIndex]);
            currentChunk = currentReader.next();
            fileIndex++;
        }

        private PartitionReader createReader(PartitionLocation location) throws IOException {
            if (location.getPeer() == null) {
                logger.warn("has only one partition replica: " + location);
            }
            if (location.getPeer() != null && attemptNumber % 2 == 1) {
                location = location.getPeer();
                logger.warn("read peer: " + location + " for attempt " + attemptNumber);
            }

            TransportClient client;
            try {
                try {
                    client = clientFactory.createClient(location.getHost(), location.getPort());
                } catch (IOException e) {
                    PartitionLocation peer = location.getPeer();
                    if (peer != null) {
                        logger.warn("connect to " + location + " failed, try to read peer " + peer);
                        location = peer;
                        client = clientFactory.createClient(location.getHost(), location.getPort());
                    } else {
                        throw e;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }

            return new PartitionReader(client, location.getFileName());
        }

        public void setCallback(MetricsCallback callback) {
            // callback must set before read()
            this.callback = callback;
        }

        @Override
        public int read() throws IOException {
            if (position < limit) {
                int b = decompressedBuf[position];
                position++;
                return b & 0xFF;
            }

            if (!fillBuffer()) {
                return -1;
            }

            if (position >= limit) {
                return read();
            } else {
                int b = decompressedBuf[position];
                position++;
                return b & 0xFF;
            }
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            if (b == null) {
                throw new NullPointerException();
            } else if (off < 0 || len < 0 || len > b.length - off) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }

            int readBytes = 0;
            while (readBytes < len) {
                while (position >= limit) {
                    if (!fillBuffer()) {
                        return readBytes > 0 ? readBytes : -1;
                    }
                }

                int bytesToRead = Math.min(limit - position, len - readBytes);
                System.arraycopy(decompressedBuf, position, b, off + readBytes, bytesToRead);
                position += bytesToRead;
                readBytes += bytesToRead;
            }

            return readBytes;
        }

        @Override
        public void close() {
            if (currentChunk != null) {
                logger.info("Release chunk!");
                currentChunk.release();
                currentChunk = null;
            }
            if (currentReader != null) {
                logger.info("Closing reader");
                currentReader.close();
                currentReader = null;
            }
        }

        private boolean moveToNextChunk() throws IOException {
            currentChunk.release();
            if (currentReader.hasNext()) {
                currentChunk = currentReader.next();
                return true;
            } else if (fileIndex < locations.length) {
                moveToNextReader();
                return true;
            }
            currentChunk = null;
            currentReader = null;
            return false;
        }

        private boolean fillBuffer() throws IOException {
            if (currentChunk == null) {
                return false;
            }

            long startTime = System.currentTimeMillis();

            boolean hasData = false;
            while (currentChunk.isReadable() || moveToNextChunk()) {
                currentChunk.readBytes(sizeBuf);
                int mapId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET);
                int attemptId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 4);
                int batchId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 8);
                int size = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 12);
                if (size > compressedBuf.length) {
                    compressedBuf = new byte[size];
                }

                currentChunk.readBytes(compressedBuf, 0, size);

                // de-duplicate
                if (attemptId == attempts[mapId]) {
                    if (!batchesRead.containsKey(mapId)) {
                        Set<Integer> batchSet = new HashSet<>();
                        batchesRead.put(mapId, batchSet);
                    }
                    Set<Integer> batchSet = batchesRead.get(mapId);
                    if (!batchSet.contains(batchId)) {
                        batchSet.add(batchId);
                        if (callback != null) {
                            callback.incBytesRead(BATCH_HEADER_SIZE + size);
                        }
                        // decompress data
                        int originalLength = decompressor.getOriginalLen(compressedBuf);
                        if (decompressedBuf.length < originalLength) {
                            decompressedBuf = new byte[originalLength];
                        }
                        limit = decompressor.decompress(compressedBuf, decompressedBuf, 0);
                        position = 0;
                        hasData = true;
                        break;
                    } else {
                        logger.warn("duplicated batch: mapId " + mapId + ", attemptId "
                            + attemptId + ", batchId " + batchId);
                    }
                }
            }

            if (callback != null) {
                callback.incReadTime(System.currentTimeMillis() - startTime);
            }
            return hasData;
        }

        private final class PartitionReader {
            private final TransportClient client;
            private final long streamId;
            private final int numChunks;

            private int returnedChunks;
            private int chunkIndex;

            private final LinkedBlockingQueue<ByteBuf> results;
            private final ChunkReceivedCallback callback;

            private final AtomicReference<IOException> exception = new AtomicReference<>();

            private final long timeoutMs = EssConf.essFetchChunkTimeoutMs(conf);

            private boolean closed = false;

            PartitionReader(TransportClient client, String fileName) throws IOException {
                this.client = client;

                ByteBuffer request = createOpenMessage(shuffleKey, fileName);
                ByteBuffer response;
                try {
                    response = client.sendRpcSync(request, timeoutMs);
                } catch (Exception e) {
                    throw new IOException("FetchChunk open stream failed", e);
                }
                streamId = response.getLong();
                numChunks = response.getInt();
                if (numChunks <= 0) {
                    throw new IOException("numChunks is " + numChunks);
                }

                results = new LinkedBlockingQueue<>();
                callback = new ChunkReceivedCallback() {
                    @Override
                    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
                        // only add the buffer to results queue if this reader is not closed.
                        synchronized(PartitionReader.this) {
                            ByteBuf buf = ((NettyManagedBuffer) buffer).getBuf();
                            if (!closed) {
                                buf.retain();
                                results.add(buf);
                            }
                        }
                    }

                    @Override
                    public void onFailure(int chunkIndex, Throwable e) {
                        logger.error(e);
                        exception.set(new IOException("FetchChunk failed", e));
                    }
                };
            }

            private ByteBuffer createOpenMessage(String shuffleKey, String fileName) {
                byte[] shuffleKeyBytes = shuffleKey.getBytes(StandardCharsets.UTF_8);
                byte[] fileNameBytes = fileName.getBytes(StandardCharsets.UTF_8);
                ByteBuffer openMessage = ByteBuffer.allocate(
                        4 + shuffleKeyBytes.length + 4 + fileNameBytes.length);
                openMessage.putInt(shuffleKeyBytes.length);
                openMessage.put(shuffleKeyBytes);
                openMessage.putInt(fileNameBytes.length);
                openMessage.put(fileNameBytes);
                openMessage.flip();
                return openMessage;
            }

            boolean hasNext() {
                return returnedChunks < numChunks;
            }

            ByteBuf next() throws IOException {
                checkException();
                if (chunkIndex < numChunks) {
                    fetchChunks();
                }
                ByteBuf chunk = null;
                try {
                    while (chunk == null) {
                        checkException();
                        chunk = results.poll(500, TimeUnit.MILLISECONDS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    IOException ioe = new IOException(e);
                    exception.set(ioe);
                    throw ioe;
                }
                returnedChunks++;
                return chunk;
            }

            void close() {
                synchronized(this) {
                    closed = true;
                }
                if (results.size() > 0) {
                    results.forEach(res -> res.release());
                }
                results.clear();
            }

            private void fetchChunks() {
                final int inFlight = chunkIndex - returnedChunks;
                if (inFlight < maxInFlight) {
                    final int toFetch = Math.min(maxInFlight - inFlight + 1, numChunks - chunkIndex);
                    for (int i = 0; i < toFetch; i++) {
                        client.fetchChunk(streamId, chunkIndex++, callback);
                    }
                }
            }

            private void checkException() throws IOException {
                IOException e = exception.get();
                if (e != null) {
                    throw e;
                }
            }
        }
    }
}
