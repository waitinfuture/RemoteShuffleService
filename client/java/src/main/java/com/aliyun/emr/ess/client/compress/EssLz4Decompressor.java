package com.aliyun.emr.ess.client.compress;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.log4j.Logger;

import java.util.zip.Checksum;

public class EssLz4Decompressor extends EssLz4CompressorTrait {
    private static Logger logger = Logger.getLogger(EssLz4Decompressor.class);
    private final LZ4FastDecompressor decompressor;
    private final Checksum checksum;

    public EssLz4Decompressor() {
        decompressor = LZ4Factory.fastestInstance().fastDecompressor();
        checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum();
    }

    public int decompress(byte[] src, byte[] dst, int dstOff) {
        int token = src[MAGIC_LENGTH] & 0xFF;
        int compressionMethod = token & 0xF0;
        int compressionLevel = COMPRESSION_LEVEL_BASE + (token & 0x0F);
        int compressedLen = readIntLE(src, MAGIC_LENGTH + 1);
        int originalLen = readIntLE(src, MAGIC_LENGTH + 5);
        int check = readIntLE(src, MAGIC_LENGTH + 9);

        switch (compressionMethod) {
            case COMPRESSION_METHOD_RAW:
                System.arraycopy(src, HEADER_LENGTH, dst, dstOff, originalLen);
                break;
            case COMPRESSION_METHOD_LZ4:
                int compressedLen2 = decompressor.decompress(
                    src, HEADER_LENGTH, dst, dstOff, originalLen);
                if (compressedLen != compressedLen2) {
                    logger.error("compressed len corrupted!");
                    return -1;
                }
        }

        checksum.reset();
        checksum.update(dst, dstOff, originalLen);
        if ((int) checksum.getValue() != check) {
            logger.error("checksum not equal!");
            return -1;
        }

        return originalLen;
    }

    public static int readIntLE(byte[] buf, int i) {
        return (buf[i] & 0xFF) | ((buf[i + 1] & 0xFF) << 8) |
            ((buf[i + 2] & 0xFF) << 16) | ((buf[i + 3] & 0xFF) << 24);
    }
}
