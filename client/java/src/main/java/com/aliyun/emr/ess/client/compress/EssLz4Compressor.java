package com.aliyun.emr.ess.client.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

import java.util.zip.Checksum;

public class EssLz4Compressor extends EssLz4CompressorTrait {
    private final int compressionLevel;
    private final LZ4Compressor compressor;
    private final Checksum checksum;
    private final byte[] compressedBuffer;
    private int compressedTotalSize;

    public EssLz4Compressor() {
        this(256 * 1024);
    }

    public EssLz4Compressor(int blockSize) {
        int level = 32 - Integer.numberOfLeadingZeros(blockSize - 1) - COMPRESSION_LEVEL_BASE;
        this.compressionLevel = Math.max(0, level);
        this.compressor = LZ4Factory.fastestInstance().fastCompressor();
        checksum = XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum();
        final int compressedBlockSize = HEADER_LENGTH + compressor.maxCompressedLength(blockSize);
        this.compressedBuffer = new byte[compressedBlockSize];
        System.arraycopy(MAGIC, 0, compressedBuffer, 0, MAGIC_LENGTH);
    }

    public void compress(byte[] data, int offset, int length) {
        checksum.reset();
        checksum.update(data, offset, length);
        final int check = (int) checksum.getValue();
        int compressedLength = compressor.compress(data, offset, length, compressedBuffer, HEADER_LENGTH);
        final int compressMethod;
        if (compressedLength >= length) {
            compressMethod = COMPRESSION_METHOD_RAW;
            compressedLength = length;
            System.arraycopy(data, offset, compressedBuffer, HEADER_LENGTH, length);
        } else {
            compressMethod = COMPRESSION_METHOD_LZ4;
        }

        compressedBuffer[MAGIC_LENGTH] = (byte) (compressMethod | compressionLevel);
        writeIntLE(compressedLength, compressedBuffer, MAGIC_LENGTH + 1);
        writeIntLE(length, compressedBuffer, MAGIC_LENGTH + 5);
        writeIntLE(check, compressedBuffer, MAGIC_LENGTH + 9);

        compressedTotalSize = HEADER_LENGTH + compressedLength;
    }

    public int getCompressedTotalSize() {
        return compressedTotalSize;
    }

    public byte[] getCompressedBuffer() {
        return compressedBuffer;
    }

    private static void writeIntLE(int i, byte[] buf, int off) {
        buf[off++] = (byte) i;
        buf[off++] = (byte) (i >>> 8);
        buf[off++] = (byte) (i >>> 16);
        buf[off++] = (byte) (i >>> 24);
    }
}
