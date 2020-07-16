package com.aliyun.emr.ess.client.stream;

import com.aliyun.emr.ess.client.MetricsCallback;
import com.aliyun.emr.ess.client.compress.EssLz4CompressorTrait;
import com.aliyun.emr.ess.client.compress.EssLz4Decompressor;
import com.aliyun.emr.ess.client.impl.ShuffleClientImpl;
import com.aliyun.emr.ess.common.EssConf;
import com.aliyun.emr.ess.common.util.EssPathUtil;
import com.aliyun.emr.ess.unsafe.Platform;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EssInputStream extends InputStream {
    private static final Logger logger = Logger.getLogger(ShuffleClientImpl.class);

    private final EssConf conf;
    private final int[] attempts;
    private final Map<Integer, Set<Integer>> batchesRead = new HashMap<>();
    private final byte[] compressedBuf;
    private final byte[] decompressedBuf;
    private final String[] filePaths;
    private FileSystem fs;
    private final EssLz4Decompressor decompressor;

    private FSDataInputStream fileInputStream;
    private int fileIndex;
    private int position;
    private int limit;

    private MetricsCallback callback;

    // EmptyInpuStream
    public static EssInputStream EmptyInputStream =
        new EssInputStream(new EssConf(), new HashSet<>(), null) {
        @Override
        public int read() throws IOException {
            return -1;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            return -1;
        }
    };

    // mapId, attempId, batchId, size
    private final byte[] sizeBuf = new byte[16];

    public EssInputStream(
        EssConf conf,
        Set<String> filePathSet,
        int[] attempts) {
        this.conf = conf;
        this.attempts = attempts;
        this.filePaths = new String[filePathSet.size()];
        int ind = 0;
        for (String path : filePathSet) {
            filePaths[ind] = path;
            ind++;
        }

        int blockSize = ((int) EssConf.essPushDataBufferSize(conf)) + EssLz4CompressorTrait
                .HEADER_LENGTH;
        compressedBuf = new byte[blockSize];
        decompressedBuf = new byte[blockSize];

        Configuration hadoopConf = new Configuration();
        Path path = EssPathUtil.GetBaseDir(conf);
        try {
            fs = path.getFileSystem(hadoopConf);
        } catch (IOException e) {
            logger.error("GetFileSystem failed!", e);
        }

        decompressor = new EssLz4Decompressor();
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
        if (fileInputStream != null) {
            fileInputStream.close();
            fileInputStream = null;
        }
    }

    private boolean fillBuffer() throws IOException {
        if (fileInputStream == null && fileIndex >= filePaths.length) {
            return false;
        }

        long startTime = System.currentTimeMillis();

        if (fileInputStream == null) {
            Path path = new Path(filePaths[fileIndex]);
            fileInputStream = fs.open(path);
        }

        try {
            fileInputStream.readFully(sizeBuf);
        } catch (Exception ex) {
            // exception means file reach eof
            fileInputStream.close();
            fileInputStream = null;
            fileIndex++;
            if (fileIndex < filePaths.length) {
                fileInputStream = fs.open(new Path(filePaths[fileIndex]));
                fileInputStream.readFully(sizeBuf);
            } else {
                return false;
            }
        }

        int mapId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET);
        int attemptId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 4);
        int batchId = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 8);
        int size = Platform.getInt(sizeBuf, Platform.BYTE_ARRAY_OFFSET + 12);

        fileInputStream.readFully(compressedBuf, 0, size);
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
            } else {
                logger.warn("duplicate batchId! " + batchId);
            }
        }

        if (callback != null) {
            callback.incReadTime(System.currentTimeMillis() - startTime);
        }

        return true;
    }
}
