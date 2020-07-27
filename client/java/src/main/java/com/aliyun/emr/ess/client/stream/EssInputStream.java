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


public abstract class EssInputStream extends InputStream {
    private static final Logger logger = Logger.getLogger(EssInputStream.class);

    public static EssInputStream create(
            EssConf conf,
            TransportClientFactory clientFactory,
            String shuffleKey,
            PartitionLocation[] locations,
            int[] attempts) throws IOException {
        if (locations == null || locations.length == 0) {
            return emptyInputStream;
        } else {
            return new EssInputStreamImpl(conf, clientFactory, shuffleKey, locations, attempts);
        }
    }

    public static EssInputStream empty() {
        return emptyInputStream;
    }

    public abstract void setCallback(MetricsCallback callback);

    private static final EssInputStream emptyInputStream =
            new EssInputStream() {
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
        private final Map<Integer, Set<Integer>> batchesRead = new HashMap<>();

        private final byte[] compressedBuf;
        private final byte[] decompressedBuf;
        private final EssLz4Decompressor decompressor;

        private ByteBuf currentChunk;
        private PartitionReader currentReader;
        private int fileIndex;
        private int position;
        private int limit;

        private MetricsCallback callback;

        // mapId, attempId, batchId, size
        private final byte[] sizeBuf = new byte[16];

        public EssInputStreamImpl(
                EssConf conf,
                TransportClientFactory clientFactory,
                String shuffleKey,
                PartitionLocation[] locations,
                int[] attempts) throws IOException {
            this.conf = conf;
            this.clientFactory = clientFactory;
            this.shuffleKey = shuffleKey;
            this.locations = locations;
            this.attempts = attempts;

            int blockSize = ((int) EssConf.essPushDataBufferSize(conf)) + EssLz4CompressorTrait
                    .HEADER_LENGTH;
            compressedBuf = new byte[blockSize];
            decompressedBuf = new byte[blockSize];

            decompressor = new EssLz4Decompressor();

            moveToNextReader();
        }

        private void moveToNextReader() throws IOException {
            currentReader = createReader(locations[fileIndex]);
            currentChunk = currentReader.next();
            fileIndex++;
        }

        private PartitionReader createReader(PartitionLocation location) throws IOException {
            try {
                TransportClient client =
                        clientFactory.createClient(location.getHost(), location.getPort());
                return new PartitionReader(client, location.getUniqueId());
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
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
        public void close() throws IOException {
            // TODO
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
                            callback.incBytesWritten(size);
                        }
                        // decompress data
                        limit = decompressor.decompress(compressedBuf, decompressedBuf, 0);
                        position = 0;
                        hasData = true;
                        break;
                    } else {
                        logger.warn("duplicate batchId! " + batchId);
                    }
                }
            }

            if (callback != null) {
                callback.incReadTime(System.currentTimeMillis() - startTime);
            }
            return hasData;
        }

        private final class PartitionReader {
            private static final int MAX_IN_FLIGHT = 3;

            private final TransportClient client;
            private final long streamId;
            private final int numChunks;

            private int returnedChunks;
            private int chunkIndex;

            private final LinkedBlockingQueue<ByteBuf> results;
            private final ChunkReceivedCallback callback;

            PartitionReader(TransportClient client, String partitionId) {
                this.client = client;

                ByteBuffer request = createOpenMessage(shuffleKey, partitionId);
                ByteBuffer response = client.sendRpcSync(request, 30 * 1000L);
                streamId = response.getLong();
                numChunks = response.getInt();

                results = new LinkedBlockingQueue<>();
                callback = new ChunkReceivedCallback() {
                    @Override
                    public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
                        ByteBuf buf = ((NettyManagedBuffer) buffer).getBuf();
                        buf.retain();
                        results.add(buf);
                    }

                    @Override
                    public void onFailure(int chunkIndex, Throwable e) {

                    }
                };
            }

            private ByteBuffer createOpenMessage(String shuffleKey, String partitionId) {
                byte[] shuffleKeyBytes = shuffleKey.getBytes(StandardCharsets.UTF_8);
                byte[] partitionIdBytes = partitionId.getBytes(StandardCharsets.UTF_8);
                ByteBuffer openMessage = ByteBuffer.allocate(
                        4 + shuffleKeyBytes.length + 4 + partitionIdBytes.length);
                openMessage.putInt(shuffleKeyBytes.length);
                openMessage.put(shuffleKeyBytes);
                openMessage.putInt(partitionIdBytes.length);
                openMessage.put(partitionIdBytes);
                openMessage.flip();
                return openMessage;
            }

            boolean hasNext() {
                return returnedChunks < numChunks;
            }

            ByteBuf next() throws IOException {
                if (chunkIndex < numChunks) {
                    fetchChunks();
                }
                returnedChunks++;
                try {
                    return results.take();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }

            private void fetchChunks() {
                final int inFlight = chunkIndex - returnedChunks - results.size();
                if (inFlight < MAX_IN_FLIGHT) {
                    final int toFetch = Math.min(MAX_IN_FLIGHT, numChunks - chunkIndex);
                    for (int i = 0; i < toFetch; i++) {
                        client.fetchChunk(streamId, chunkIndex++, callback);
                    }
                }
            }
        }
    }
}
