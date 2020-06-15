package com.aliyun.emr.ess.client.compress;

public abstract class EssLz4CompressorTrait {
    protected static final byte[] MAGIC = new byte[] { 'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k' };
    protected static final int MAGIC_LENGTH = MAGIC.length;

    public static final int HEADER_LENGTH =
        MAGIC_LENGTH // magic bytes
            + 1          // token
            + 4          // compressed length
            + 4          // decompressed length
            + 4;         // checksum

    protected static final int COMPRESSION_LEVEL_BASE = 10;

    protected static final int COMPRESSION_METHOD_RAW = 0x10;
    protected static final int COMPRESSION_METHOD_LZ4 = 0x20;

    protected static final int DEFAULT_SEED = 0x9747b28c;
}
