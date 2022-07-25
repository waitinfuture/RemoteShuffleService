/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.emr.rss.service.deploy.worker;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import scala.Function0;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import com.aliyun.emr.rss.common.RssConf;
import com.aliyun.emr.rss.common.network.TransportContext;
import com.aliyun.emr.rss.common.network.buffer.ManagedBuffer;
import com.aliyun.emr.rss.common.network.client.ChunkReceivedCallback;
import com.aliyun.emr.rss.common.network.client.TransportClient;
import com.aliyun.emr.rss.common.network.client.TransportClientFactory;
import com.aliyun.emr.rss.common.network.protocol.Message;
import com.aliyun.emr.rss.common.network.protocol.OpenStreamReduce;
import com.aliyun.emr.rss.common.network.protocol.StreamHandleReduce;
import com.aliyun.emr.rss.common.network.server.FileInfo;
import com.aliyun.emr.rss.common.network.server.MemoryTracker;
import com.aliyun.emr.rss.common.network.server.TransportServer;
import com.aliyun.emr.rss.common.network.util.JavaUtils;
import com.aliyun.emr.rss.common.network.util.MapConfigProvider;
import com.aliyun.emr.rss.common.network.util.TransportConf;
import com.aliyun.emr.rss.common.protocol.PartitionSplitMode;
import com.aliyun.emr.rss.common.protocol.PartitionType;
import com.aliyun.emr.rss.common.util.ThreadUtils;
import com.aliyun.emr.rss.common.util.Utils;

public class FileWriterSuiteJ {

  private static final Logger LOG = LoggerFactory.getLogger(FileWriterSuiteJ.class);

  private static final int CHUNK_SIZE = 1024;
  private static final int FLUSH_TIMEOUT = 240 * 1000; // 240s
  public static final int FLUSH_BUFFER_SIZE_LIMIT = 256 * 1024; //256KB
  public static final Long SPLIT_THRESHOLD = 256 * 1024 * 1024L;
  public static final PartitionSplitMode splitMode = PartitionSplitMode.hard;
  public static final PartitionType partitionType= PartitionType.REDUCE_PARTITION;

  private static File tempDir = null;
  private static DiskFlusher flusher = null;
  private static WorkerSource source = null;

  private static TransportServer server;
  private static TransportClientFactory clientFactory;
  private static long streamId;
  private static int numChunks;

  private static final TransportConf transConf =
    new TransportConf("shuffle", MapConfigProvider.EMPTY);

  @BeforeClass
  public static void beforeAll() {
    tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "rss");

    source = Mockito.mock(WorkerSource.class);
    Mockito.doAnswer(invocationOnMock -> {
      Function0<?> function = (Function0<?>) invocationOnMock.getArguments()[2];
      return function.apply();
    }).when(source)
      .sample(Mockito.anyString(), Mockito.anyString(), Mockito.any(Function0.class));

    flusher = new DiskFlusher(tempDir, 100, source, DeviceMonitor$.MODULE$.EmptyMonitor(), 2);
    MemoryTracker.initialize(0.8, 0.9, 0.5, 0.6, 10, 10, 10);
  }

  public static void setupChunkServer(FileInfo info) throws Exception {
    FetchHandler handler = new FetchHandler(transConf) {
      @Override
      public FileInfo openStream(
        String shuffleKey,
        String fileName,
        int startMapIndex,
        int endMapIndex) {
        return info;
      }

      @Override
      public WorkerSource source() {
        return source;
      }

      @Override
      public boolean checkRegistered() {
        return true;
      }
    };
    TransportContext context = new TransportContext(transConf, handler);
    server = context.createServer();

    clientFactory = context.createClientFactory();
  }

  @AfterClass
  public static void afterAll() {
    if (tempDir != null) {
      try {
        JavaUtils.deleteRecursively(tempDir);
        tempDir = null;
      } catch (IOException e) {
        LOG.error("Failed to delete temp dir.", e);
      }
    }
  }

  public static void closeChunkServer() {
    server.close();
    clientFactory.close();
  }

  static class FetchResult {
    public Set<Integer> successChunks;
    public Set<Integer> failedChunks;
    public List<ManagedBuffer> buffers;

    public void releaseBuffers() {
      for (ManagedBuffer buffer : buffers) {
        buffer.release();
      }
    }
  }

  public ByteBuffer createOpenMessage() {
    byte[] shuffleKeyBytes = "shuffleKey".getBytes(StandardCharsets.UTF_8);
    byte[] fileNameBytes = "location".getBytes(StandardCharsets.UTF_8);

    OpenStreamReduce openBlocks = new OpenStreamReduce(shuffleKeyBytes, fileNameBytes,
      0, Integer.MAX_VALUE);

    return openBlocks.toByteBuffer();
  }

  private void setUpConn(TransportClient client) {
    ByteBuffer resp = client.sendRpcSync(createOpenMessage(), 10000);
    StreamHandleReduce streamHandleReduce = (StreamHandleReduce) Message.decode(resp);
    streamId = streamHandleReduce.streamId;
    numChunks = streamHandleReduce.numChunks;
  }

  private FetchResult fetchChunks(TransportClient client,
                                  List<Integer> chunkIndices) throws Exception {
    final Semaphore sem = new Semaphore(0);

    final FetchResult res = new FetchResult();
    res.successChunks = Collections.synchronizedSet(new HashSet<Integer>());
    res.failedChunks = Collections.synchronizedSet(new HashSet<Integer>());
    res.buffers = Collections.synchronizedList(new LinkedList<ManagedBuffer>());

    ChunkReceivedCallback callback = new ChunkReceivedCallback() {
      @Override
      public void onSuccess(int chunkIndex, ManagedBuffer buffer) {
        buffer.retain();
        res.successChunks.add(chunkIndex);
        res.buffers.add(buffer);
        sem.release();
      }

      @Override
      public void onFailure(int chunkIndex, Throwable e) {
        res.failedChunks.add(chunkIndex);
        sem.release();
      }
    };

    for (int chunkIndex : chunkIndices) {
      client.fetchChunk(streamId, chunkIndex, callback);
    }
    if (!sem.tryAcquire(chunkIndices.size(), 5, TimeUnit.SECONDS)) {
      fail("Timeout getting response from the server");
    }

    client.close();
    return res;
  }

  @Test
  public void testMultiThreadWrite() throws IOException, ExecutionException, InterruptedException {
    final int threadsNum = 8;
    File file = getTemporaryFile();
    FileWriter writer = new FileWriter(file, flusher, file.getParentFile(), CHUNK_SIZE,
      FLUSH_BUFFER_SIZE_LIMIT, source, new RssConf(),
      DeviceMonitor$.MODULE$.EmptyMonitor(), SPLIT_THRESHOLD, splitMode, partitionType);

    List<Future<?>> futures = new ArrayList<>();
    ExecutorService es = ThreadUtils.newDaemonFixedThreadPool(threadsNum, "FileWriter-UT-1");
    AtomicLong length = new AtomicLong(0);

    for (int i = 0; i < threadsNum; ++i) {
      futures.add(es.submit(() -> {
        byte[] bytes = generateData();
        length.addAndGet(bytes.length);
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        try {
          writer.write(buf);
        } catch (IOException e) {
          LOG.error("Failed to write buffer.", e);
        }
      }));
    }
    for (Future<?> future : futures) {
      future.get();
    }

    long bytesWritten = writer.close();

    assertEquals(length.get(), bytesWritten);
    assertEquals(writer.getFile().length(), bytesWritten);
  }

  @Test
  public void testAfterStressfulWriteWillReadCorrect()
    throws IOException, ExecutionException, InterruptedException {
    final int threadsNum = Runtime.getRuntime().availableProcessors();
    File file = getTemporaryFile();
    FileWriter writer = new FileWriter(file, flusher, file.getParentFile(), CHUNK_SIZE,
      FLUSH_BUFFER_SIZE_LIMIT, source, new RssConf(),
      DeviceMonitor$.MODULE$.EmptyMonitor(), SPLIT_THRESHOLD, splitMode, partitionType);

    List<Future<?>> futures = new ArrayList<>();
    ExecutorService es = ThreadUtils.newDaemonFixedThreadPool(threadsNum, "FileWriter-UT-2");
    AtomicLong length = new AtomicLong(0);
    for (int i = 0; i < threadsNum; ++i) {
      futures.add(es.submit(() -> {
        for (int j = 0; j < 100; ++j) {
          byte[] bytes = generateData();
          length.addAndGet(bytes.length);
          ByteBuf buf = Unpooled.wrappedBuffer(bytes);
          try {
            writer.write(buf);
          } catch (IOException e) {
            LOG.error("Failed to write buffer.", e);
          }
        }
      }));
    }
    for (Future<?> future : futures) {
      future.get();
    }

    long bytesWritten = writer.close();
    assertEquals(length.get(), bytesWritten);
  }

  @Test
  public void testHugeBufferQueueSize() throws IOException {
    File file = getTemporaryFile();
    flusher = new DiskFlusher(file, 100_0000, source, DeviceMonitor$.MODULE$.EmptyMonitor(), 2);
  }

  @Test
  public void testWriteAndChunkRead() throws Exception {
    final int threadsNum = 8;
    File file = getTemporaryFile();
    FileWriter writer = new FileWriter(file, flusher, file.getParentFile(), CHUNK_SIZE,
      FLUSH_BUFFER_SIZE_LIMIT, source, new RssConf(),
      DeviceMonitor$.MODULE$.EmptyMonitor(), SPLIT_THRESHOLD, splitMode, partitionType);

    List<Future<?>> futures = new ArrayList<>();
    ExecutorService es = ThreadUtils.newDaemonFixedThreadPool(threadsNum, "FileWriter-UT-2");
    AtomicLong length = new AtomicLong(0);
    futures.add(es.submit(() -> {
      for (int j = 0; j < 1000; ++j) {
        byte[] bytes = generateData();
        length.addAndGet(bytes.length);
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        buf.retain();
        try {
          writer.incrementPendingWrites();
          writer.write(buf);
        } catch (IOException e) {
          LOG.error("Failed to write buffer.", e);
        }
        buf.release();
      }
    }));
    for (Future<?> future : futures) {
      future.get();
    }

    long bytesWritten = writer.close();
    assertEquals(length.get(), bytesWritten);

    FileInfo fileInfo = new FileInfo(writer.getFile(), writer.getChunkOffsets());

    setupChunkServer(fileInfo);

    TransportClient client = clientFactory.createClient(InetAddress
      .getLocalHost().getHostAddress(), server.getPort());

    setUpConn(client);

    int chunkNums = numChunks;
    List<Integer> indices = new ArrayList<>();

    for (int i = 0; i < chunkNums; i++) {
      indices.add(i);
    }

    FetchResult result = fetchChunks(client, indices);

    Thread.sleep(3000);

    assertEquals(result.buffers.size(), chunkNums);
    assertEquals(result.successChunks.size(), chunkNums);

    result.releaseBuffers();

    closeChunkServer();
  }

  @Test
  public void testCompositeBufClear() {
    ByteBuf buf = Unpooled.wrappedBuffer("hello world".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf2 = Unpooled.wrappedBuffer("hello world".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf3 = Unpooled.wrappedBuffer("hello world".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf4 = Unpooled.wrappedBuffer("hello world".getBytes(StandardCharsets.UTF_8));
    ByteBuf buf5 = buf4.duplicate();

    buf5.retain();
    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
    compositeByteBuf.addComponent(true,buf);
    compositeByteBuf.addComponent(true,buf2);
    compositeByteBuf.addComponent(true,buf3);
    compositeByteBuf.addComponent(true,buf4);
    compositeByteBuf.addComponent(true,buf5);

    assertEquals(1,buf.refCnt());
    assertEquals(1,buf2.refCnt());
    assertEquals(1,buf3.refCnt());
    assertEquals(2,buf4.refCnt());
    assertEquals(2,buf5.refCnt());
    assertNotEquals(0,compositeByteBuf.readableBytes());

    compositeByteBuf.removeComponents(0, compositeByteBuf.numComponents());
    compositeByteBuf.clear();

    assertEquals(0,compositeByteBuf.readableBytes());

    assertEquals(0,buf.refCnt());
    assertEquals(0,buf2.refCnt());
    assertEquals(0,buf3.refCnt());
    assertEquals(0,buf4.refCnt());
    assertEquals(0,buf5.refCnt());
    assertEquals(0,compositeByteBuf.numComponents());
  }

  private File getTemporaryFile() throws IOException {
    String filename = UUID.randomUUID().toString();
    File temporaryFile = new File(tempDir, filename);
    temporaryFile.createNewFile();
    return temporaryFile;
  }

  private byte[] generateData() {
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    byte[] hello = "hello, world".getBytes(StandardCharsets.UTF_8);
    int tempLen = rand.nextInt(256 * 1024) + 128 * 1024;
    int len = (int) (Math.ceil(1.0 * tempLen / hello.length) * hello.length);

    byte[] data = new byte[len];
    for (int i = 0; i < len; i += hello.length) {
      System.arraycopy(hello, 0, data, i, hello.length);
    }
    return data;
  }
}
