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

package com.aliyun.emr.rss.client.write;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import com.aliyun.emr.rss.client.ShuffleClient;
import com.aliyun.emr.rss.common.RssConf;

public class DataPusher {
  private final long WAIT_TIME_NANOS = TimeUnit.MILLISECONDS.toNanos(500);

  private final LinkedBlockingQueue<PushTask> idleQueue;
  private final LinkedBlockingQueue<PushTask> workingQueue;

  private final ReentrantLock idleLock = new ReentrantLock();
  private final Condition idleFull = idleLock.newCondition();

  private final AtomicReference<IOException> exception = new AtomicReference<>();

  private final String appId;
  private final int shuffleId;
  private final int mapId;
  private final int attemptId;
  private final int numMappers;
  private final int numPartitions;
  private final ShuffleClient client;
  private final Consumer<Integer> afterPush;

  private volatile boolean terminated;
  private LongAdder[] mapStatusLengths;

  public DataPusher(
      String appId,
      int shuffleId,
      int mapId,
      int attemptId,
      long taskId,
      int numMappers,
      int numPartitions,
      RssConf conf,
      ShuffleClient client,
      Consumer<Integer> afterPush,
      LongAdder[] mapStatusLengths) throws IOException {
    final int capacity = RssConf.pushDataQueueCapacity(conf);
    final int bufferSize = RssConf.pushDataBufferSize(conf);

    idleQueue = new LinkedBlockingQueue<>(capacity);
    workingQueue = new LinkedBlockingQueue<>(capacity);

    for (int i = 0; i < capacity; i++) {
      try {
        idleQueue.put(new PushTask(bufferSize));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    this.appId = appId;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptId = attemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.client = client;
    this.afterPush = afterPush;
    this.mapStatusLengths = mapStatusLengths;

    new Thread("DataPusher-" + taskId) {
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
        while (!terminated && exception.get() == null) {
          try {
            PushTask task = workingQueue.poll(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
            if (task == null) {
              continue;
            }
            pushData(task);
            reclaimTask(task);
          } catch (InterruptedException e) {
            exception.set(new IOException(e));
          } catch (IOException e) {
            exception.set(e);
          }
        }
      }
    }.start();
  }

  public void addTask(int partitionId, byte[] buffer, int size) throws IOException {
    try {
      PushTask task = null;
      while (task == null) {
        checkException();
        task = idleQueue.poll(WAIT_TIME_NANOS, TimeUnit.NANOSECONDS);
      }
      task.setSize(size);
      task.setPartitionId(partitionId);
      System.arraycopy(buffer, 0, task.getBuffer(), 0, size);
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

  public void waitOnTermination() throws IOException {
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

  private void pushData(PushTask task) throws IOException {
    int length =
      client.compressInplace(shuffleId, mapId, attemptId, task.getBuffer(), 0, task.getSize());
    byte[] compressed = new byte[length];
    System.arraycopy(task.getBuffer(), 0, compressed, 0, length);
    int bytesWritten = client.pushData(
        appId,
        shuffleId,
        mapId,
        attemptId,
        task.getPartitionId(),
        compressed,
        numMappers,
        numPartitions
    );
    afterPush.accept(bytesWritten);
    mapStatusLengths[task.getPartitionId()].add(bytesWritten);
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
