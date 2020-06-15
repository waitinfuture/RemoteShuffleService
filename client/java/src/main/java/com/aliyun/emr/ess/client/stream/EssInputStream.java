package com.aliyun.emr.ess.client.stream;

import com.aliyun.emr.ess.client.MetricsCallback;
import com.aliyun.emr.ess.client.compress.EssLz4CompressorTrait;
import com.aliyun.emr.ess.client.compress.EssLz4Decompressor;
import com.aliyun.emr.ess.client.impl.ShuffleClientImpl;
import com.aliyun.emr.ess.common.EssConf;
import com.aliyun.emr.ess.common.util.EssPathUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EssInputStream extends InputStream {
    private static Logger logger = Logger.getLogger(ShuffleClientImpl.class);

    private EssConf conf;
    private int[] attempts;
    private Map<Integer, Set<Integer>> batchesRead = new HashMap<>();
    private int blockSize;
    private byte[] compressedBuf;
    private byte[] decompressedBuf;
    private int position = 0;
    private int limit = 0;
    private String[] filePaths;
    private int fileIndex = 0;
    private FileSystem fs = null;
    private FSDataInputStream fileInputStream = null;
    private EssLz4Decompressor decompressor;
    private MetricsCallback callback = null;

    // mapId, attempId, batchId, size
    private byte[] sizeBuf = new byte[16];

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

        blockSize = ((int) EssConf.essPushDataBufferSize(conf)) + EssLz4CompressorTrait
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
        this.callback = callback;
    }

    public void setCallback(MetricsCallback callback)
    {
        // callback must set before read()
        this.callback = callback;
        logger.info("MetricsCallback set!");
    }

    @Override
    public int read() throws IOException {
        if (position < limit) {
            int b = decompressedBuf[position];
            position++;
            return b & 0xFF;
        }

        if (fileInputStream == null && fileIndex >= filePaths.length) {
            return -1;
        }

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
            fileIndex ++;
            if (fileIndex < filePaths.length) {
                fileInputStream = fs.open(new Path(filePaths[fileIndex]));
                fileInputStream.readFully(sizeBuf);
            } else {
                return -1;
            }
        }

        ByteBuffer byteBuffer = ByteBuffer.wrap(sizeBuf);
        int mapId = byteBuffer.getInt();
        int attempId = byteBuffer.getInt();
        int batchId = byteBuffer.getInt();
        int size = byteBuffer.getInt();

        fileInputStream.readFully(compressedBuf, 0, size);
        // de-duplicate
        if (attempId == attempts[mapId]) {
            if (!batchesRead.containsKey(mapId)) {
                Set<Integer> batchSet = new HashSet<>();
                batchesRead.put(mapId, batchSet);
            }
            Set<Integer> batchSet = batchesRead.get(mapId);
            if (!batchSet.contains(batchId)) {
                batchSet.add(batchId);
                if (callback != null) {
                    callback.bytesWritten((long) size);
                }
                // decompress data
                limit = decompressor.decompress(compressedBuf, decompressedBuf, 0);
                position = 0;
            }
        }

        if (position >= limit) {
            return read();
        } else {
            int b = decompressedBuf[position];
            position++;
            return b & 0xFF;
        }
    }
}