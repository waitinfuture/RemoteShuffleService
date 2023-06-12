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

package org.apache.celeborn.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import scala.reflect.ClassTag$;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.compress.Compressor;
import org.apache.celeborn.client.read.RssInputStream;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.haclient.RssHARetryClient;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.PushData;
import org.apache.celeborn.common.network.protocol.PushMergedData;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.*;
import org.apache.celeborn.common.protocol.message.ControlMessages.*;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.rpc.RpcAddress;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.rpc.RpcEnv;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.*;
import org.apache.celeborn.common.write.DataBatches;
import org.apache.celeborn.common.write.PushState;

public class ShuffleClientImpl extends ShuffleClient {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleClientImpl.class);

  protected static final byte MASTER_MODE = PartitionLocation.Mode.MASTER.mode();

  private static final Random RND = new Random();

  protected final CelebornConf conf;

  private final UserIdentifier userIdentifier;

  private final int registerShuffleMaxRetries;
  private final long registerShuffleRetryWaitMs;
  private int maxReviveTimes;
  private boolean testRetryRevive;
  private final int pushBufferMaxSize;
  protected final long pushDataTimeout;

  private final RpcEnv rpcEnv;

  protected RpcEndpointRef driverRssMetaService;

  protected TransportClientFactory dataClientFactory;

  protected final int BATCH_HEADER_SIZE = 4 * 4;

  // key: shuffleId, value: (partitionId, PartitionLocation)
  private final Map<Integer, ConcurrentHashMap<Integer, PartitionLocation>> reducePartitionMap =
      JavaUtils.newConcurrentHashMap();

  protected final ConcurrentHashMap<Integer, Set<String>> mapperEndMap =
      JavaUtils.newConcurrentHashMap();

  // key: shuffleId-mapId-attemptId
  protected final Map<String, PushState> pushStates = JavaUtils.newConcurrentHashMap();

  private final boolean shuffleClientPushBlacklistEnabled;
  private final Set<String> blacklist = ConcurrentHashMap.newKeySet();

  private final ExecutorService pushDataRetryPool;

  private final ExecutorService partitionSplitPool;
  private final Map<Integer, Set<Integer>> splitting = JavaUtils.newConcurrentHashMap();

  ThreadLocal<Compressor> compressorThreadLocal =
      new ThreadLocal<Compressor>() {
        @Override
        protected Compressor initialValue() {
          return Compressor.getCompressor(conf);
        }
      };

  ReviveManager reviveManager = new ReviveManager();

  protected static class ReduceFileGroups {
    public Map<Integer, Set<PartitionLocation>> partitionGroups;
    public int[] mapAttempts;
    public Set<Integer> partitionIds;

    ReduceFileGroups(
        Map<Integer, Set<PartitionLocation>> partitionGroups,
        int[] mapAttempts,
        Set<Integer> partitionIds) {
      this.partitionGroups = partitionGroups;
      this.mapAttempts = mapAttempts;
      this.partitionIds = partitionIds;
    }

    public ReduceFileGroups() {
      this.partitionGroups = null;
      this.mapAttempts = null;
      this.partitionIds = null;
    }

    public void update(ReduceFileGroups fileGroups) {
      partitionGroups = fileGroups.partitionGroups;
      mapAttempts = fileGroups.mapAttempts;
      partitionIds = fileGroups.partitionIds;
    }
  }

  // key: shuffleId
  protected final Map<Integer, ReduceFileGroups> reduceFileGroupsMap =
      JavaUtils.newConcurrentHashMap();

  public ShuffleClientImpl(CelebornConf conf, UserIdentifier userIdentifier) {
    super();
    this.conf = conf;
    this.userIdentifier = userIdentifier;
    registerShuffleMaxRetries = conf.clientRegisterShuffleMaxRetry();
    registerShuffleRetryWaitMs = conf.clientRegisterShuffleRetryWaitMs();
    maxReviveTimes = conf.clientPushMaxReviveTimes();
    testRetryRevive = conf.testRetryRevive();
    pushBufferMaxSize = conf.clientPushBufferMaxSize();
    shuffleClientPushBlacklistEnabled = conf.clientPushBlacklistEnabled();
    if (conf.clientPushReplicateEnabled()) {
      pushDataTimeout = conf.pushDataTimeoutMs() * 2;
    } else {
      pushDataTimeout = conf.pushDataTimeoutMs();
    }

    // init rpc env and master endpointRef
    rpcEnv = RpcEnv.create("ShuffleClient", Utils.localHostName(), 0, conf);

    String module = TransportModuleConstants.DATA_MODULE;
    TransportConf dataTransportConf =
        Utils.fromCelebornConf(conf, module, conf.getInt("celeborn" + module + ".io.threads", 8));
    TransportContext context =
        new TransportContext(
            dataTransportConf, new BaseMessageHandler(), conf.clientCloseIdleConnections());
    dataClientFactory = context.createClientFactory();

    int pushDataRetryThreads = conf.clientPushRetryThreads();
    pushDataRetryPool =
        ThreadUtils.newDaemonCachedThreadPool("celeborn-retry-sender", pushDataRetryThreads, 60);

    int pushSplitPartitionThreads = conf.clientPushSplitPartitionThreads();
    partitionSplitPool =
        ThreadUtils.newDaemonCachedThreadPool(
            "celeborn-shuffle-split", pushSplitPartitionThreads, 60);
  }

  private boolean checkPushBlacklisted(
      PartitionLocation location, RpcResponseCallback wrappedCallback) {
    // If shuffleClientBlacklistEnabled = false, blacklist should be empty.
    if (blacklist.contains(location.hostAndPushPort())) {
      wrappedCallback.onFailure(new CelebornIOException(StatusCode.PUSH_DATA_MASTER_BLACKLISTED));
      return true;
    } else if (location.getPeer() != null
        && blacklist.contains(location.getPeer().hostAndPushPort())) {
      wrappedCallback.onFailure(new CelebornIOException(StatusCode.PUSH_DATA_SLAVE_BLACKLISTED));
      return true;
    } else {
      return false;
    }
  }

  private void submitRetryPushData(
      String applicationId,
      int shuffleId,
      byte[] body,
      int batchId,
      RpcResponseCallback wrappedCallback,
      PushState pushState,
      ReviveRequest request,
      int remainReviveTimes) {
    int mapId = request.mapId;
    int attemptId = request.attemptId;
    PartitionLocation loc = request.loc;
    StatusCode cause = request.cause;
    int partitionId = loc.getId();
    long reviveWaitTime =
        conf.clientRpcRequestPartitionLocationsRpcAskTimeout().duration().toMillis() + 1000;
    final long delta = 50;
    long accumulatedTime = 0;
    while (request.reviveStatus == StatusCode.REVIVE_INITIALIZED
        && accumulatedTime <= reviveWaitTime) {
      try {
        Thread.sleep(50);
        accumulatedTime += delta;
      } catch (InterruptedException e) {
        logger.error("Interrupted while waiting for Revive result!");
        wrappedCallback.onFailure(
            new CelebornIOException(cause + " then revive but " + StatusCode.REVIVE_FAILED));
        return;
      }
    }
    if (mapperEnded(shuffleId, mapId, attemptId)) {
      logger.debug(
          "Revive for push data success, but the mapper already ended for shuffle {} map {} attempt {} partition {} batch {} location {}.",
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          batchId,
          loc);
      pushState.removeBatch(batchId, loc.hostAndPushPort());
    } else if (request.reviveStatus != StatusCode.SUCCESS) {
      wrappedCallback.onFailure(
        new CelebornIOException(cause + " then revive but " + StatusCode.REVIVE_FAILED));
    }
    else {
      PartitionLocation newLoc = reducePartitionMap.get(shuffleId).get(partitionId);
      logger.debug(
          "Revive for push data success, new location for shuffle {} map {} attempt {} partition {} batch {} is location {}.",
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          batchId,
          newLoc);
      try {
        if (!checkPushBlacklisted(newLoc, wrappedCallback)) {
          if (!testRetryRevive || remainReviveTimes < 1) {
            TransportClient client =
                dataClientFactory.createClient(newLoc.getHost(), newLoc.getPushPort(), partitionId);
            NettyManagedBuffer newBuffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
            String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);

            PushData newPushData =
                new PushData(MASTER_MODE, shuffleKey, newLoc.getUniqueId(), newBuffer);
            client.pushData(newPushData, pushDataTimeout, wrappedCallback);
          } else {
            throw new RuntimeException(
                "Mock push data submit retry failed. remainReviveTimes = "
                    + remainReviveTimes
                    + ".");
          }
        }
      } catch (Exception e) {
        logger.error(
            "Exception raised while pushing data for shuffle {} map {} attempt {} partition {} batch {} location {}.",
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            batchId,
            newLoc,
            e);
        wrappedCallback.onFailure(
            new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER, e));
      }
    }
  }

  public ReviveRequest[] addAndGetReviveRequests(
      String appId,
      int shuffleId,
      int mapId,
      int attemptId,
      ArrayList<DataBatches.DataBatch> batches,
      StatusCode cause) {
    ReviveRequest[] reviveRequests = new ReviveRequest[batches.size()];
    for (int i = 0; i < batches.size(); i++) {
      DataBatches.DataBatch batch = batches.get(i);
      PartitionLocation loc = batch.loc;
      ReviveRequest reviveRequest =
          new ReviveRequest(mapId, attemptId, loc.getId(), loc.getEpoch(), loc, cause);
      reviveManager.addRequest(appId, shuffleId, reviveRequest);
      reviveRequests[i] = reviveRequest;
    }
    return reviveRequests;
  }

  private void submitRetryPushMergedData(
      PushState pushState,
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      ArrayList<DataBatches.DataBatch> batches,
      StatusCode cause,
      Integer oldGroupedBatchId,
      ReviveRequest[] reviveRequests,
      int remainReviveTimes) {
    HashMap<String, DataBatches> newDataBatchesMap = new HashMap<>();
    ArrayList<DataBatches.DataBatch> reviveFailedBatchesMap = new ArrayList<>();

    long reviveWaitTime =
        conf.clientRpcRequestPartitionLocationsRpcAskTimeout().duration().toMillis() + 1000;
    final long delta = 50;
    long accumulatedTime = 0;
    int index = 0;
    while (index < reviveRequests.length && accumulatedTime <= reviveWaitTime) {
      ReviveRequest request = reviveRequests[index];
      DataBatches.DataBatch batch = batches.get(index);
      if (request.reviveStatus != StatusCode.REVIVE_INITIALIZED) {
        if (request.reviveStatus == StatusCode.SUCCESS) {
          PartitionLocation newLoc = reducePartitionMap.get(shuffleId).get(request.partitionId);
          DataBatches newDataBatches =
              newDataBatchesMap.computeIfAbsent(genAddressPair(newLoc), (s) -> new DataBatches());
          newDataBatches.addDataBatch(newLoc, batch.batchId, batch.body);
        } else if (mapperEnded(shuffleId, mapId, attemptId)) {
          logger.debug(
              "Revive for push merged data success, but the mapper already ended for shuffle {} map {} attempt {} partition {} batch {}.",
              shuffleId,
              mapId,
              attemptId,
              request.partitionId,
              oldGroupedBatchId);
        } else {
          if (remainReviveTimes > 0) {
            reviveFailedBatchesMap.add(batch);
          } else {
            String errorMsg =
                String.format(
                    "Revive failed while pushing merged for shuffle %d map %d attempt %d partition %d batch %d location %s.",
                    shuffleId, mapId, attemptId, request.partitionId, oldGroupedBatchId, batch.loc);
            pushState.exception.compareAndSet(
                null,
                new CelebornIOException(
                    errorMsg,
                    new CelebornIOException(
                        cause + " then revive but " + StatusCode.REVIVE_FAILED)));
            return;
          }
        }

        index++;
      } else {
        try {
          Thread.sleep(delta);
        } catch (InterruptedException e) {
          logger.warn("Interrupted while waiting for ReviveBatch result!");
        }
        accumulatedTime += delta;
      }
    }

    for (int i = index; i < reviveRequests.length; i++) {
      ReviveRequest request = reviveRequests[index];
      DataBatches.DataBatch batch = batches.get(i);
      if (remainReviveTimes > 0) {
        reviveFailedBatchesMap.add(batch);
      } else {
        String errorMsg =
            String.format(
                "Revive failed while pushing merged for shuffle %d map %d attempt %d partition %d batch %d location %s.",
                shuffleId, mapId, attemptId, request.partitionId, oldGroupedBatchId, batch.loc);
        pushState.exception.compareAndSet(
            null,
            new CelebornIOException(
                errorMsg,
                new CelebornIOException(cause + " then revive but " + StatusCode.REVIVE_FAILED)));
        return;
      }
    }

    for (Map.Entry<String, DataBatches> entry : newDataBatchesMap.entrySet()) {
      String addressPair = entry.getKey();
      DataBatches newDataBatches = entry.getValue();
      doPushMergedData(
          addressPair,
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          newDataBatches.requireBatches(),
          pushState,
          remainReviveTimes);
    }
    if (reviveFailedBatchesMap.isEmpty()) {
      pushState.removeBatch(oldGroupedBatchId, batches.get(0).loc.hostAndPushPort());
    } else {
      pushDataRetryPool.submit(
          () ->
              submitRetryPushMergedData(
                  pushState,
                  applicationId,
                  shuffleId,
                  mapId,
                  attemptId,
                  reviveFailedBatchesMap,
                  cause,
                  oldGroupedBatchId,
                  addAndGetReviveRequests(
                      applicationId, shuffleId, mapId, attemptId, reviveFailedBatchesMap, cause),
                  remainReviveTimes - 1));
    }
  }

  private String genAddressPair(PartitionLocation loc) {
    String addressPair;
    if (loc.getPeer() != null) {
      addressPair = loc.hostAndPushPort() + "-" + loc.getPeer().hostAndPushPort();
    } else {
      addressPair = loc.hostAndPushPort();
    }
    return addressPair;
  }

  private ConcurrentHashMap<Integer, PartitionLocation> registerShuffle(
      String appId, int shuffleId, int numMappers, int numPartitions) {
    return registerShuffleInternal(
        shuffleId,
        numMappers,
        numPartitions,
        () ->
            driverRssMetaService.askSync(
                RegisterShuffle$.MODULE$.apply(appId, shuffleId, numMappers, numPartitions),
                conf.clientRpcRegisterShuffleRpcAskTimeout(),
                ClassTag$.MODULE$.apply(PbRegisterShuffleResponse.class)));
  }

  @VisibleForTesting
  public PartitionLocation registerMapPartitionTask(
      String appId, int shuffleId, int numMappers, int mapId, int attemptId, int partitionId)
      throws IOException {
    logger.info(
        "Register MapPartition task for shuffle {} map {} attempt {} partition {} with {} mapper.",
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        numMappers);
    ConcurrentHashMap<Integer, PartitionLocation> partitionLocationMap =
        registerShuffleInternal(
            shuffleId,
            numMappers,
            numMappers,
            () ->
                driverRssMetaService.askSync(
                    RegisterMapPartitionTask$.MODULE$.apply(
                        appId, shuffleId, numMappers, mapId, attemptId, partitionId),
                    conf.clientRpcRegisterShuffleRpcAskTimeout(),
                    ClassTag$.MODULE$.apply(PbRegisterShuffleResponse.class)));

    if (partitionLocationMap == null) {
      String shuffleKey = Utils.makeShuffleKey(appId, shuffleId);
      throw new CelebornIOException("Register shuffle failed for shuffle " + shuffleKey);
    }

    return partitionLocationMap.get(partitionId);
  }

  @Override
  public ConcurrentHashMap<Integer, PartitionLocation> getPartitionLocation(
      String applicationId, int shuffleId, int numMappers, int numPartitions) {
    return reducePartitionMap.computeIfAbsent(
        shuffleId, (id) -> registerShuffle(applicationId, shuffleId, numMappers, numPartitions));
  }

  @Override
  public PushState getPushState(String mapKey) {
    return pushStates.computeIfAbsent(mapKey, (s) -> new PushState(conf));
  }

  private ConcurrentHashMap<Integer, PartitionLocation> registerShuffleInternal(
      int shuffleId,
      int numMappers,
      int numPartitions,
      Callable<PbRegisterShuffleResponse> callable) {
    int numRetries = registerShuffleMaxRetries;
    while (numRetries > 0) {
      try {
        PbRegisterShuffleResponse response = callable.call();
        StatusCode respStatus = Utils.toStatusCode(response.getStatus());
        if (StatusCode.SUCCESS.equals(respStatus)) {
          ConcurrentHashMap<Integer, PartitionLocation> result = JavaUtils.newConcurrentHashMap();
          for (int i = 0; i < response.getPartitionLocationsList().size(); i++) {
            PartitionLocation partitionLoc =
                PbSerDeUtils.fromPbPartitionLocation(response.getPartitionLocationsList().get(i));
            result.put(partitionLoc.getId(), partitionLoc);
          }
          return result;
        } else if (StatusCode.SLOT_NOT_AVAILABLE.equals(respStatus)) {
          logger.error(
              "LifecycleManager request slots return {}, retry again, remain retry times {}.",
              StatusCode.SLOT_NOT_AVAILABLE,
              numRetries - 1);
        } else if (StatusCode.RESERVE_SLOTS_FAILED.equals(respStatus)) {
          logger.error(
              "LifecycleManager request slots return {}, retry again, remain retry times {}.",
              StatusCode.RESERVE_SLOTS_FAILED,
              numRetries - 1);
        } else {
          logger.error(
              "LifecycleManager request slots return {}, retry again, remain retry times {}.",
              StatusCode.REQUEST_FAILED,
              numRetries - 1);
        }
      } catch (Exception e) {
        logger.error(
            "Exception raised while registering shuffle {} with {} mapper and {} partitions.",
            shuffleId,
            numMappers,
            numPartitions,
            e);
        break;
      }

      try {
        TimeUnit.MILLISECONDS.sleep(registerShuffleRetryWaitMs);
      } catch (InterruptedException e) {
        break;
      }
      numRetries--;
    }

    return null;
  }

  protected void limitMaxInFlight(String mapKey, PushState pushState, String hostAndPushPort)
      throws IOException {
    boolean reachLimit = pushState.limitMaxInFlight(hostAndPushPort);

    if (reachLimit) {
      throw new CelebornIOException(
          "Waiting timeout for task " + mapKey, pushState.exception.get());
    }
  }

  protected void limitZeroInFlight(String mapKey, PushState pushState) throws IOException {
    boolean reachLimit = pushState.limitZeroInFlight();

    if (reachLimit) {
      throw new CelebornIOException(
          "Waiting timeout for task " + mapKey, pushState.exception.get());
    }
  }

  private boolean checkRevivedLocation(int shuffleId, int partitionId, int epoch, boolean wait) {
    ConcurrentHashMap<Integer, PartitionLocation> map = reducePartitionMap.get(shuffleId);
    PartitionLocation currentLocation = map.get(partitionId);
    if (currentLocation != null && currentLocation.getEpoch() > epoch) {
      return true;
    }
    if (wait) {
      long sleepTimeMs = RND.nextInt(50);
      if (sleepTimeMs > 30) {
        try {
          TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          logger.error("Waiting revived location was interrupted.", e);
          Thread.currentThread().interrupt();
        }
      }

      currentLocation = map.get(partitionId);
      return currentLocation != null && currentLocation.getEpoch() > epoch;
    } else {
      return false;
    }
  }

  private void blacklistByCause(StatusCode cause, PartitionLocation oldLocation) {
    // Add ShuffleClient side blacklist
    if (shuffleClientPushBlacklistEnabled) {
      if (cause == StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER) {
        blacklist.add(oldLocation.hostAndPushPort());
      } else if (cause == StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER) {
        blacklist.add(oldLocation.hostAndPushPort());
      } else if (cause == StatusCode.PUSH_DATA_TIMEOUT_MASTER) {
        blacklist.add(oldLocation.hostAndPushPort());
      } else if (cause == StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE) {
        blacklist.add(oldLocation.getPeer().hostAndPushPort());
      } else if (cause == StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE) {
        blacklist.add(oldLocation.getPeer().hostAndPushPort());
      } else if (cause == StatusCode.PUSH_DATA_TIMEOUT_SLAVE) {
        blacklist.add(oldLocation.getPeer().hostAndPushPort());
      }
    }
  }

  private boolean revive(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      int epoch,
      PartitionLocation oldLocation,
      StatusCode cause) {
    blacklistByCause(cause, oldLocation);

    ConcurrentHashMap<Integer, PartitionLocation> map = reducePartitionMap.get(shuffleId);

    if (checkRevivedLocation(shuffleId, partitionId, epoch, true)) {
      logger.debug(
          "Revive already success for shuffle {} map {} attempt {} partition {} epoch {}, just return true(Assume revive successfully).",
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          epoch);
      return true;
    }
    String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    if (mapperEnded(shuffleId, mapId, attemptId)) {
      logger.debug(
          "Revive success, but the mapper ended for shuffle {} map {} attempt {} partition {}, just return true(Assume revive successfully).",
          shuffleId,
          mapId,
          attemptId,
          partitionId);
      return true;
    }

    try {
      PbChangeLocationsResponse response =
          driverRssMetaService.askSync(
              Revive$.MODULE$.apply(
                  applicationId,
                  shuffleId,
                  mapId,
                  attemptId,
                  partitionId,
                  epoch,
                  oldLocation,
                  cause),
              conf.clientRpcRequestPartitionLocationRpcAskTimeout(),
              ClassTag$.MODULE$.apply(PbChangeLocationsResponse.class));
      // per partitionKey only serve single PartitionLocation in Client Cache.
      StatusCode respStatus = Utils.toStatusCode(response.getStatuses(0));
      if (StatusCode.SUCCESS.equals(respStatus)) {
        map.put(partitionId, PbSerDeUtils.fromPbPartitionLocation(response.getLocations(0)));
        return true;
      } else if (StatusCode.MAP_ENDED.equals(respStatus)) {
        logger.debug(
            "Revive success, but the mapper ended for shuffle {} map {} attempt {} partition {}, just return true(Assume revive successfully).",
            shuffleId,
            mapId,
            attemptId,
            partitionId);
        mapperEndMap.computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet()).add(mapKey);
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      logger.error(
          "Exception raised while reviving for shuffle {} map {} attempt {} partition {} epoch {}.",
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          epoch,
          e);
      return false;
    }
  }

  /** @return mapId -> attemptId -> partitionId -> StatusCode */
  private Map<Integer, Map<Integer, Map<Integer, StatusCode>>> reviveBatch(
      String applicationId,
      int shuffleId,
      int[] mapIds,
      int[] attemptIds,
      int[] partitionIds,
      int[] epochs,
      PartitionLocation[] oldLocations,
      StatusCode[] causes) {
    Map<Integer, Map<Integer, Map<Integer, StatusCode>>> results = new HashMap<>();

    ConcurrentHashMap<Integer, PartitionLocation> map = reducePartitionMap.get(shuffleId);

    try {
      PbChangeLocationsResponse response =
          driverRssMetaService.askSync(
              ReviveBatch$.MODULE$.apply(
                  applicationId,
                  shuffleId,
                  mapIds,
                  attemptIds,
                  partitionIds,
                  epochs,
                  oldLocations,
                  causes),
              conf.clientRpcRequestPartitionLocationsRpcAskTimeout(),
              ClassTag$.MODULE$.apply(PbChangeLocationsResponse.class));

      for (int i = 0; i < response.getMapIdsCount(); i++) {
        int mapId = response.getMapIds(i);
        int attemptId = response.getAttemptIds(i);
        int partitionId = response.getPartitionIds(i);
        StatusCode statusCode = Utils.toStatusCode(response.getStatuses(i));
        PartitionLocation loc = PbSerDeUtils.fromPbPartitionLocation(response.getLocations(i));

        if (StatusCode.SUCCESS.equals(statusCode)) {
          map.put(partitionId, loc);
        } else if (StatusCode.MAP_ENDED.equals(statusCode)) {
          String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
          mapperEndMap
              .computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet())
              .add(mapKey);
        }

        Map<Integer, Map<Integer, StatusCode>> attemptIdMap =
            results.computeIfAbsent(mapId, (id) -> new HashMap<>());
        Map<Integer, StatusCode> partitionIdMap =
            attemptIdMap.computeIfAbsent(attemptId, (id) -> new HashMap<>());
        partitionIdMap.put(partitionId, statusCode);
      }

      return results;
    } catch (Exception e) {
      logger.error(
          "Exception raised while reviving for shuffle {} reduce {} epoch {}.",
          shuffleId,
          partitionIds,
          epochs,
          e);
      return null;
    }
  }

  public int pushOrMergeData(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions,
      boolean doPush)
      throws IOException {
    // mapKey
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    final String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
    // return if shuffle stage already ended
    if (mapperEnded(shuffleId, mapId, attemptId)) {
      logger.debug(
          "Push or merge data ignored because mapper already ended for shuffle {} map {} attempt {} partition {}.",
          shuffleId,
          mapId,
          attemptId,
          partitionId);
      PushState pushState = pushStates.get(mapKey);
      if (pushState != null) {
        pushState.cleanup();
      }
      return 0;
    }
    // register shuffle if not registered
    final ConcurrentHashMap<Integer, PartitionLocation> map =
        getPartitionLocation(applicationId, shuffleId, numMappers, numPartitions);

    if (map == null) {
      throw new CelebornIOException("Register shuffle failed for shuffle " + shuffleKey + ".");
    }

    // get location
    // If rerun or speculation task running after LifecycleManager call stageEnd,
    // register shuffle will return an empty location map, client need revive for a new location.
    if (!map.containsKey(partitionId)) {
      if (!revive(
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          -1,
          null,
          StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE)) {
        throw new CelebornIOException(
            String.format("Revive for shuffle %s partition %d failed.", shuffleKey, partitionId));
      }
    }

    if (mapperEnded(shuffleId, mapId, attemptId)) {
      logger.debug(
          "Push or merge data ignored because mapper already ended for shuffle {} map {} attempt {} partition {}.",
          shuffleId,
          mapId,
          attemptId,
          partitionId);
      PushState pushState = pushStates.get(mapKey);
      if (pushState != null) {
        pushState.cleanup();
      }
      return 0;
    }

    final PartitionLocation loc = map.get(partitionId);
    if (loc == null) {
      throw new CelebornIOException(
          String.format(
              "Partition location for shuffle %s partition %d is NULL!", shuffleKey, partitionId));
    }

    PushState pushState = getPushState(mapKey);

    // increment batchId
    final int nextBatchId = pushState.nextBatchId();

    // compress data
    final Compressor compressor = compressorThreadLocal.get();
    compressor.compress(data, offset, length);

    final int compressedTotalSize = compressor.getCompressedTotalSize();

    final byte[] body = new byte[BATCH_HEADER_SIZE + compressedTotalSize];
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET, mapId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 4, attemptId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 8, nextBatchId);
    Platform.putInt(body, Platform.BYTE_ARRAY_OFFSET + 12, compressedTotalSize);
    System.arraycopy(
        compressor.getCompressedBuffer(), 0, body, BATCH_HEADER_SIZE, compressedTotalSize);

    if (doPush) {
      // check limit
      limitMaxInFlight(mapKey, pushState, loc.hostAndPushPort());

      // add inFlight requests
      pushState.addBatch(nextBatchId, loc.hostAndPushPort());

      // build PushData request
      NettyManagedBuffer buffer = new NettyManagedBuffer(Unpooled.wrappedBuffer(body));
      PushData pushData = new PushData(MASTER_MODE, shuffleKey, loc.getUniqueId(), buffer);

      // build callback
      RpcResponseCallback callback =
          new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
              pushState.removeBatch(nextBatchId, loc.hostAndPushPort());
              // TODO Need to adjust maxReqsInFlight if server response is congested, see
              // CELEBORN-62
              if (response.remaining() > 0 && response.get() == StatusCode.STAGE_ENDED.getValue()) {
                mapperEndMap
                    .computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet())
                    .add(mapKey);
              }
              logger.debug(
                  "Push data to {} success for shuffle {} map {} attempt {} partition {} batch {}.",
                  loc.hostAndPushPort(),
                  shuffleId,
                  mapId,
                  attemptId,
                  partitionId,
                  nextBatchId);
            }

            @Override
            public void onFailure(Throwable e) {
              String errorMsg =
                  String.format(
                      "Push data to %s failed for shuffle %d map %d attempt %d partition %d batch %d.",
                      loc, shuffleId, mapId, attemptId, partitionId, nextBatchId);
              pushState.exception.compareAndSet(null, new CelebornIOException(errorMsg, e));
            }
          };

      RpcResponseCallback wrappedCallback =
          new RpcResponseCallback() {
            int remainReviveTimes = maxReviveTimes;

            @Override
            public void onSuccess(ByteBuffer response) {
              if (response.remaining() > 0) {
                byte reason = response.get();
                if (reason == StatusCode.SOFT_SPLIT.getValue()) {
                  logger.debug(
                      "Push data to {} soft split required for shuffle {} map {} attempt {} partition {} batch {}.",
                      loc.hostAndPushPort(),
                      shuffleId,
                      mapId,
                      attemptId,
                      partitionId,
                      nextBatchId);
                  splitPartition(shuffleId, partitionId, applicationId, loc);
                  pushState.onSuccess(loc.hostAndPushPort());
                  callback.onSuccess(response);
                } else if (reason == StatusCode.HARD_SPLIT.getValue()) {
                  logger.debug(
                      "Push data to {} hard split required for shuffle {} map {} attempt {} partition {} batch {}.",
                      loc.hostAndPushPort(),
                      shuffleId,
                      mapId,
                      attemptId,
                      partitionId,
                      nextBatchId);
                  ReviveRequest reviveRequest =
                      new ReviveRequest(
                          mapId,
                          attemptId,
                          partitionId,
                          loc.getEpoch(),
                          loc,
                          StatusCode.HARD_SPLIT);
                  reviveManager.addRequest(applicationId, shuffleId, reviveRequest);
                  pushDataRetryPool.submit(
                      () ->
                          submitRetryPushData(
                              applicationId,
                              shuffleId,
                              body,
                              nextBatchId,
                              this,
                              pushState,
                              reviveRequest,
                              remainReviveTimes));
                } else if (reason == StatusCode.PUSH_DATA_SUCCESS_MASTER_CONGESTED.getValue()) {
                  logger.debug(
                      "Push data to {} master congestion required for shuffle {} map {} attempt {} partition {} batch {}.",
                      loc.hostAndPushPort(),
                      shuffleId,
                      mapId,
                      attemptId,
                      partitionId,
                      nextBatchId);
                  pushState.onCongestControl(loc.hostAndPushPort());
                  callback.onSuccess(response);
                } else if (reason == StatusCode.PUSH_DATA_SUCCESS_SLAVE_CONGESTED.getValue()) {
                  logger.debug(
                      "Push data to {} slave congestion required for shuffle {} map {} attempt {} partition {} batch {}.",
                      loc.hostAndPushPort(),
                      shuffleId,
                      mapId,
                      attemptId,
                      partitionId,
                      nextBatchId);
                  pushState.onCongestControl(loc.hostAndPushPort());
                  callback.onSuccess(response);
                } else {
                  // StageEnd.
                  response.rewind();
                  pushState.onSuccess(loc.hostAndPushPort());
                  callback.onSuccess(response);
                }
              } else {
                pushState.onSuccess(loc.hostAndPushPort());
                callback.onSuccess(response);
              }
            }

            @Override
            public void onFailure(Throwable e) {
              StatusCode cause = getPushDataFailCause(e.getMessage());

              if (pushState.exception.get() != null) {
                return;
              }

              if (remainReviveTimes <= 0) {
                if (e instanceof CelebornIOException) {
                  callback.onFailure(e);
                } else {
                  callback.onFailure(new CelebornIOException(cause, e));
                }
                return;
              }

              logger.error(
                  "Push data to {} failed for shuffle {} map {} attempt {} partition {} batch {}, remain revive times {}.",
                  loc.hostAndPushPort(),
                  shuffleId,
                  mapId,
                  attemptId,
                  partitionId,
                  nextBatchId,
                  remainReviveTimes,
                  e);
              // async retry push data
              if (!mapperEnded(shuffleId, mapId, attemptId)) {
                // For blacklisted partition location, Celeborn should not use retry quota.
                if (!pushStatusIsBlacklisted(cause)) {
                  remainReviveTimes = remainReviveTimes - 1;
                }
                ReviveRequest reviveRequest =
                    new ReviveRequest(
                        mapId, attemptId, partitionId, loc.getEpoch(), loc, cause);
                reviveManager.addRequest(applicationId, shuffleId, reviveRequest);
                pushDataRetryPool.submit(
                    () ->
                        submitRetryPushData(
                            applicationId,
                            shuffleId,
                            body,
                            nextBatchId,
                            this,
                            pushState,
                            reviveRequest,
                            remainReviveTimes));
              } else {
                pushState.removeBatch(nextBatchId, loc.hostAndPushPort());
                logger.info(
                    "Push data to {} failed but mapper already ended for shuffle {} map {} attempt {} partition {} batch {}, remain revive times {}.",
                    loc.hostAndPushPort(),
                    shuffleId,
                    mapId,
                    attemptId,
                    partitionId,
                    nextBatchId,
                    remainReviveTimes);
              }
            }
          };

      // do push data
      try {
        if (!checkPushBlacklisted(loc, wrappedCallback)) {
          if (!testRetryRevive) {
            TransportClient client =
                dataClientFactory.createClient(loc.getHost(), loc.getPushPort(), partitionId);
            client.pushData(pushData, pushDataTimeout, wrappedCallback);
          } else {
            wrappedCallback.onFailure(
                new CelebornIOException(
                    StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE,
                    new RuntimeException("Mock push data first time failed.")));
          }
        }
      } catch (Exception e) {
        logger.error(
            "Exception raised while pushing data for shuffle {} map {} attempt {} partition {} batch {} location {}.",
            shuffleId,
            mapId,
            attemptId,
            partitionId,
            nextBatchId,
            loc,
            e);
        wrappedCallback.onFailure(
            new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER, e));
      }
    } else {
      // add batch data
      logger.debug("Merge batch {}.", nextBatchId);
      String addressPair = genAddressPair(loc);
      boolean shouldPush = pushState.addBatchData(addressPair, loc, nextBatchId, body);
      if (shouldPush) {
        limitMaxInFlight(mapKey, pushState, loc.hostAndPushPort());
        DataBatches dataBatches = pushState.takeDataBatches(addressPair);
        doPushMergedData(
            addressPair,
            applicationId,
            shuffleId,
            mapId,
            attemptId,
            dataBatches.requireBatches(),
            pushState,
            maxReviveTimes);
      }
    }

    return body.length;
  }

  private void splitPartition(
      int shuffleId, int partitionId, String applicationId, PartitionLocation loc) {
    Set<Integer> splittingSet =
        splitting.computeIfAbsent(shuffleId, integer -> ConcurrentHashMap.newKeySet());
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (splittingSet) {
      if (splittingSet.contains(partitionId)) {
        logger.debug(
            "Splitting for shuffle {} partition {}, skip split request.", shuffleId, partitionId);
        return;
      }
      splittingSet.add(partitionId);
    }

    ConcurrentHashMap<Integer, PartitionLocation> currentShuffleLocs =
        reducePartitionMap.get(shuffleId);

    ShuffleClientHelper.sendShuffleSplitAsync(
        driverRssMetaService,
        conf,
        PartitionSplit$.MODULE$.apply(applicationId, shuffleId, partitionId, loc.getEpoch(), loc),
        partitionSplitPool,
        splittingSet,
        partitionId,
        shuffleId,
        currentShuffleLocs);
  }

  @Override
  public int pushData(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException {
    return pushOrMergeData(
        applicationId,
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        data,
        offset,
        length,
        numMappers,
        numPartitions,
        true);
  }

  @Override
  public void prepareForMergeData(int shuffleId, int mapId, int attemptId) throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = pushStates.get(mapKey);
    if (pushState != null) {
      limitZeroInFlight(mapKey, pushState);
    }
  }

  @Override
  public int mergeData(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException {
    return pushOrMergeData(
        applicationId,
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        data,
        offset,
        length,
        numMappers,
        numPartitions,
        false);
  }

  public void pushMergedData(String applicationId, int shuffleId, int mapId, int attemptId)
      throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = pushStates.get(mapKey);
    if (pushState == null) {
      return;
    }
    ArrayList<Map.Entry<String, DataBatches>> batchesArr =
        new ArrayList<>(pushState.batchesMap.entrySet());
    while (!batchesArr.isEmpty()) {
      Map.Entry<String, DataBatches> entry = batchesArr.get(RND.nextInt(batchesArr.size()));
      String[] tokens = entry.getKey().split("-");
      limitMaxInFlight(mapKey, pushState, tokens[0]);
      ArrayList<DataBatches.DataBatch> batches = entry.getValue().requireBatches(pushBufferMaxSize);
      if (entry.getValue().getTotalSize() == 0) {
        batchesArr.remove(entry);
      }
      doPushMergedData(
          entry.getKey(),
          applicationId,
          shuffleId,
          mapId,
          attemptId,
          batches,
          pushState,
          maxReviveTimes);
    }
  }

  private void doPushMergedData(
      String addressPair,
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      ArrayList<DataBatches.DataBatch> batches,
      PushState pushState,
      int remainReviveTimes) {
    String hostPort = addressPair.split("-")[0];
    final String[] splits = hostPort.split(":");
    final String host = splits[0];
    final int port = Integer.parseInt(splits[1]);

    int groupedBatchId = pushState.nextBatchId();
    pushState.addBatch(groupedBatchId, hostPort);

    final int numBatches = batches.size();
    final Integer[] partitionIds = new Integer[numBatches];
    final String[] partitionUniqueIds = new String[numBatches];
    final int[] offsets = new int[numBatches];
    final int[] batchIds = new int[numBatches];
    int currentSize = 0;
    CompositeByteBuf byteBuf = Unpooled.compositeBuffer();
    for (int i = 0; i < numBatches; i++) {
      DataBatches.DataBatch batch = batches.get(i);
      partitionIds[i] = batch.loc.getId();
      partitionUniqueIds[i] = batch.loc.getUniqueId();
      offsets[i] = currentSize;
      batchIds[i] = batch.batchId;
      currentSize += batch.body.length;
      byteBuf.addComponent(true, Unpooled.wrappedBuffer(batch.body));
    }
    NettyManagedBuffer buffer = new NettyManagedBuffer(byteBuf);
    String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
    PushMergedData mergedData =
        new PushMergedData(MASTER_MODE, shuffleKey, partitionUniqueIds, offsets, buffer);

    RpcResponseCallback callback =
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer response) {
            logger.debug(
                "Push merged data to {} success for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}.",
                addressPair,
                shuffleId,
                mapId,
                attemptId,
                Arrays.toString(partitionIds),
                groupedBatchId,
                Arrays.toString(batchIds));
            pushState.removeBatch(groupedBatchId, hostPort);
            // TODO Need to adjust maxReqsInFlight if server response is congested, see CELEBORN-62
            if (response.remaining() > 0 && response.get() == StatusCode.STAGE_ENDED.getValue()) {
              mapperEndMap
                  .computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet())
                  .add(Utils.makeMapKey(shuffleId, mapId, attemptId));
            }
          }

          @Override
          public void onFailure(Throwable e) {
            String errorMsg =
                String.format(
                    "Push merged data to %s failed for shuffle %d map %d attempt %d partition %s groupedBatch %d batch %s, remain revive times %d.",
                    addressPair,
                    shuffleId,
                    mapId,
                    attemptId,
                    Arrays.toString(partitionIds),
                    groupedBatchId,
                    Arrays.toString(batchIds),
                    remainReviveTimes);
            pushState.exception.compareAndSet(null, new CelebornIOException(errorMsg, e));
            if (logger.isDebugEnabled()) {
              for (int i = 0; i < numBatches; i++) {
                logger.debug(
                    "Push merged data to {} failed for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}, remain revive times {}.",
                    addressPair,
                    shuffleId,
                    mapId,
                    attemptId,
                    partitionIds[i],
                    groupedBatchId,
                    batchIds[i],
                    remainReviveTimes);
              }
            }
          }
        };

    RpcResponseCallback wrappedCallback =
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer response) {
            if (response.remaining() > 0) {
              byte reason = response.get();
              if (reason == StatusCode.HARD_SPLIT.getValue()) {
                logger.info(
                    "Push merged data to {} hard split required for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}.",
                    addressPair,
                    shuffleId,
                    mapId,
                    attemptId,
                    Arrays.toString(partitionIds),
                    groupedBatchId,
                    Arrays.toString(batchIds));
                pushDataRetryPool.submit(
                    () ->
                        submitRetryPushMergedData(
                            pushState,
                            applicationId,
                            shuffleId,
                            mapId,
                            attemptId,
                            batches,
                            StatusCode.HARD_SPLIT,
                            groupedBatchId,
                            addAndGetReviveRequests(
                                applicationId,
                                shuffleId,
                                mapId,
                                attemptId,
                                batches,
                                StatusCode.HARD_SPLIT),
                            remainReviveTimes));
              } else if (reason == StatusCode.PUSH_DATA_SUCCESS_MASTER_CONGESTED.getValue()) {
                logger.debug(
                    "Push merged data to {} master congestion required for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}.",
                    addressPair,
                    shuffleId,
                    mapId,
                    attemptId,
                    Arrays.toString(partitionIds),
                    groupedBatchId,
                    Arrays.toString(batchIds));
                pushState.onCongestControl(hostPort);
                callback.onSuccess(response);
              } else if (reason == StatusCode.PUSH_DATA_SUCCESS_SLAVE_CONGESTED.getValue()) {
                logger.debug(
                    "Push merged data to {} slave congestion required for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}.",
                    addressPair,
                    shuffleId,
                    mapId,
                    attemptId,
                    Arrays.toString(partitionIds),
                    groupedBatchId,
                    Arrays.toString(batchIds));
                pushState.onCongestControl(hostPort);
                callback.onSuccess(response);
              } else {
                // StageEnd.
                response.rewind();
                pushState.onSuccess(hostPort);
                callback.onSuccess(response);
              }
            } else {
              pushState.onSuccess(hostPort);
              callback.onSuccess(response);
            }
          }

          @Override
          public void onFailure(Throwable e) {
            StatusCode cause = getPushDataFailCause(e.getMessage());

            if (pushState.exception.get() != null) {
              return;
            }
            if (remainReviveTimes <= 0) {
              if (e instanceof CelebornIOException) {
                callback.onFailure(e);
              } else {
                callback.onFailure(new CelebornIOException(cause, e));
              }
              return;
            }
            logger.error(
                "Push merged data to {} failed for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}, remain revive times {}.",
                addressPair,
                shuffleId,
                mapId,
                attemptId,
                Arrays.toString(partitionIds),
                groupedBatchId,
                Arrays.toString(batchIds),
                remainReviveTimes,
                e);
            if (!mapperEnded(shuffleId, mapId, attemptId)) {
              int tmpRemainReviveTimes = remainReviveTimes;
              // For blacklisted partition location, Celeborn should not use retry quota.
              if (!pushStatusIsBlacklisted(cause)) {
                tmpRemainReviveTimes = tmpRemainReviveTimes - 1;
              }
              int finalRemainReviveTimes = tmpRemainReviveTimes;

              pushDataRetryPool.submit(
                  () ->
                      submitRetryPushMergedData(
                          pushState,
                          applicationId,
                          shuffleId,
                          mapId,
                          attemptId,
                          batches,
                          cause,
                          groupedBatchId,
                          addAndGetReviveRequests(
                              applicationId, shuffleId, mapId, attemptId, batches, cause),
                          finalRemainReviveTimes));
            } else {
              pushState.removeBatch(groupedBatchId, hostPort);
              logger.info(
                  "Push merged data to {} failed but mapper already ended for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {}, remain revive times {}.",
                  hostPort,
                  shuffleId,
                  mapId,
                  attemptId,
                  Arrays.toString(partitionIds),
                  groupedBatchId,
                  Arrays.toString(batchIds),
                  remainReviveTimes);
            }
          }
        };

    // do push merged data
    try {
      if (!checkPushBlacklisted(batches.get(0).loc, wrappedCallback)) {
        if (!testRetryRevive || remainReviveTimes < 1) {
          TransportClient client = dataClientFactory.createClient(host, port);
          client.pushMergedData(mergedData, pushDataTimeout, wrappedCallback);
        } else {
          wrappedCallback.onFailure(
              new CelebornIOException(
                  StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE,
                  new RuntimeException("Mock push merge data failed.")));
        }
      }
    } catch (Exception e) {
      logger.error(
          "Exception raised while pushing merged data for shuffle {} map {} attempt {} partition {} groupedBatch {} batch {} location {}.",
          shuffleId,
          mapId,
          attemptId,
          Arrays.toString(partitionIds),
          groupedBatchId,
          Arrays.toString(batchIds),
          addressPair,
          e);
      wrappedCallback.onFailure(
          new CelebornIOException(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER, e));
    }
  }

  @Override
  public void mapperEnd(
      String applicationId, int shuffleId, int mapId, int attemptId, int numMappers)
      throws IOException {
    mapEndInternal(applicationId, shuffleId, mapId, attemptId, numMappers, -1);
  }

  @Override
  public void mapPartitionMapperEnd(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int partitionId)
      throws IOException {
    mapEndInternal(applicationId, shuffleId, mapId, attemptId, numMappers, partitionId);
  }

  private void mapEndInternal(
      String applicationId,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      Integer partitionId)
      throws IOException {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = getPushState(mapKey);

    try {
      limitZeroInFlight(mapKey, pushState);

      MapperEndResponse response =
          driverRssMetaService.askSync(
              new MapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers, partitionId),
              ClassTag$.MODULE$.apply(MapperEndResponse.class));
      if (response.status() != StatusCode.SUCCESS) {
        throw new CelebornIOException("MapperEnd failed! StatusCode: " + response.status());
      }
    } finally {
      pushStates.remove(mapKey);
    }
  }

  @Override
  public void cleanup(String applicationId, int shuffleId, int mapId, int attemptId) {
    final String mapKey = Utils.makeMapKey(shuffleId, mapId, attemptId);
    PushState pushState = pushStates.remove(mapKey);
    if (pushState != null) {
      pushState.exception.compareAndSet(null, new CelebornIOException("Cleaned Up"));
      pushState.cleanup();
    }
  }

  @Override
  public boolean unregisterShuffle(String applicationId, int shuffleId, boolean isDriver) {
    if (isDriver) {
      try {
        driverRssMetaService.send(
            UnregisterShuffle$.MODULE$.apply(
                applicationId, shuffleId, RssHARetryClient.genRequestId()));
      } catch (Exception e) {
        // If some exceptions need to be ignored, they shouldn't be logged as error-level,
        // otherwise it will mislead users.
        logger.error("Send UnregisterShuffle failed, ignore.", e);
      }
    }

    // clear status
    reducePartitionMap.remove(shuffleId);
    reduceFileGroupsMap.remove(shuffleId);
    mapperEndMap.remove(shuffleId);
    splitting.remove(shuffleId);

    logger.info("Unregistered shuffle {}.", shuffleId);
    return true;
  }

  @Override
  public RssInputStream readPartition(
      String applicationId, int shuffleId, int partitionId, int attemptNumber) throws IOException {
    return readPartition(
        applicationId, shuffleId, partitionId, attemptNumber, 0, Integer.MAX_VALUE);
  }

  protected ReduceFileGroups loadFileGroupInternal(
      String applicationId, String shuffleKey, int shuffleId) {
    {
      long getReducerFileGroupStartTime = System.nanoTime();
      try {
        if (driverRssMetaService == null) {
          logger.warn("Driver endpoint is null!");
          return null;
        }

        GetReducerFileGroup getReducerFileGroup = new GetReducerFileGroup(applicationId, shuffleId);

        GetReducerFileGroupResponse response =
            driverRssMetaService.askSync(
                getReducerFileGroup,
                conf.clientRpcGetReducerFileGroupRpcAskTimeout(),
                ClassTag$.MODULE$.apply(GetReducerFileGroupResponse.class));

        if (response.status() == StatusCode.SUCCESS) {
          logger.info(
              "Shuffle {} request reducer file group success using {} ms, result partition size {}.",
              shuffleId,
              (System.nanoTime() - getReducerFileGroupStartTime) / 1000_000,
              response.fileGroup().size());
          return new ReduceFileGroups(
              response.fileGroup(), response.attempts(), response.partitionIds());
        } else if (response.status() == StatusCode.STAGE_END_TIME_OUT) {
          logger.warn(
              "Request {} return {} for {}.",
              getReducerFileGroup,
              StatusCode.STAGE_END_TIME_OUT,
              shuffleKey);
        } else if (response.status() == StatusCode.SHUFFLE_DATA_LOST) {
          logger.warn(
              "Request {} return {} for {}.",
              getReducerFileGroup,
              StatusCode.SHUFFLE_DATA_LOST,
              shuffleKey);
        }
      } catch (Exception e) {
        logger.error("Exception raised while call GetReducerFileGroup for {}.", shuffleKey, e);
      }
      return null;
    }
  }

  protected ReduceFileGroups updateFileGroup(
      String applicationId, String shuffleKey, int shuffleId, int partitionId) throws IOException {
    return reduceFileGroupsMap.computeIfAbsent(
        shuffleId, (id) -> loadFileGroupInternal(applicationId, shuffleKey, shuffleId));
  }

  protected ReduceFileGroups loadFileGroup(
      String applicationId, String shuffleKey, int shuffleId, int partitionId) throws IOException {
    ReduceFileGroups reduceFileGroups =
        updateFileGroup(applicationId, shuffleKey, shuffleId, partitionId);
    if (reduceFileGroups == null) {
      String msg =
          "Shuffle data lost for shuffle " + shuffleId + " partitionId " + partitionId + "!";
      logger.error(msg);
      throw new CelebornIOException(msg);
    }
    return reduceFileGroups;
  }

  @Override
  public RssInputStream readPartition(
      String applicationId,
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex)
      throws IOException {
    String shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId);
    ReduceFileGroups fileGroups = loadFileGroup(applicationId, shuffleKey, shuffleId, partitionId);

    if (fileGroups.partitionGroups.size() == 0
        || !fileGroups.partitionGroups.containsKey(partitionId)) {
      logger.warn("Shuffle data is empty for shuffle {} partition {}.", shuffleId, partitionId);
      return RssInputStream.empty();
    } else {
      return RssInputStream.create(
          conf,
          dataClientFactory,
          shuffleKey,
          fileGroups.partitionGroups.get(partitionId).toArray(new PartitionLocation[0]),
          fileGroups.mapAttempts,
          attemptNumber,
          startMapIndex,
          endMapIndex);
    }
  }

  @VisibleForTesting
  public Map<Integer, ReduceFileGroups> getReduceFileGroupsMap() {
    return reduceFileGroupsMap;
  }

  @Override
  public void shutdown() {
    if (null != rpcEnv) {
      rpcEnv.shutdown();
    }
    if (null != dataClientFactory) {
      dataClientFactory.close();
    }
    if (null != pushDataRetryPool) {
      pushDataRetryPool.shutdown();
    }
    if (null != partitionSplitPool) {
      partitionSplitPool.shutdown();
    }
    if (null != driverRssMetaService) {
      driverRssMetaService = null;
    }
    logger.warn("Shuffle client has been shutdown!");
  }

  @Override
  public void setupMetaServiceRef(String host, int port) {
    driverRssMetaService =
        rpcEnv.setupEndpointRef(new RpcAddress(host, port), RpcNameConstants.RSS_METASERVICE_EP);
  }

  @Override
  public void setupMetaServiceRef(RpcEndpointRef endpointRef) {
    driverRssMetaService = endpointRef;
  }

  protected boolean mapperEnded(int shuffleId, int mapId, int attemptId) {
    return mapperEndMap.containsKey(shuffleId)
        && mapperEndMap.get(shuffleId).contains(Utils.makeMapKey(shuffleId, mapId, attemptId));
  }

  private boolean pushStatusIsBlacklisted(StatusCode cause) {
    return cause == StatusCode.PUSH_DATA_MASTER_BLACKLISTED
        || cause == StatusCode.PUSH_DATA_SLAVE_BLACKLISTED;
  }

  private StatusCode getPushDataFailCause(String message) {
    logger.debug("Push data failed cause message: " + message);
    StatusCode cause;
    if (message == null) {
      logger.error("Push data throw unexpected exception: {}", message);
      cause = StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE;
    } else if (message.startsWith(StatusCode.PUSH_DATA_WRITE_FAIL_SLAVE.name())) {
      cause = StatusCode.PUSH_DATA_WRITE_FAIL_SLAVE;
    } else if (message.startsWith(StatusCode.PUSH_DATA_WRITE_FAIL_MASTER.name())) {
      cause = StatusCode.PUSH_DATA_WRITE_FAIL_MASTER;
    } else if (message.startsWith(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER.name())) {
      cause = StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_MASTER;
    } else if (message.startsWith(StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE.name())) {
      cause = StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_SLAVE;
    } else if (message.startsWith(StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER.name())) {
      cause = StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER;
    } else if (message.startsWith(StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE.name())) {
      cause = StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_SLAVE;
    } else if (message.startsWith(StatusCode.PUSH_DATA_TIMEOUT_MASTER.name())) {
      cause = StatusCode.PUSH_DATA_TIMEOUT_MASTER;
    } else if (message.startsWith(StatusCode.PUSH_DATA_TIMEOUT_SLAVE.name())) {
      cause = StatusCode.PUSH_DATA_TIMEOUT_SLAVE;
    } else if (message.startsWith(StatusCode.REPLICATE_DATA_FAILED.name())) {
      cause = StatusCode.REPLICATE_DATA_FAILED;
    } else if (message.startsWith(StatusCode.PUSH_DATA_MASTER_BLACKLISTED.name())) {
      cause = StatusCode.PUSH_DATA_MASTER_BLACKLISTED;
    } else if (message.startsWith(StatusCode.PUSH_DATA_SLAVE_BLACKLISTED.name())) {
      cause = StatusCode.PUSH_DATA_SLAVE_BLACKLISTED;
    } else if (connectFail(message)) {
      // Throw when push to master worker connection causeException.
      cause = StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_MASTER;
    } else {
      cause = StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE;
    }
    return cause;
  }

  private boolean connectFail(String message) {
    return (message.startsWith("Connection from ") && message.endsWith(" closed"))
        || (message.equals("Connection reset by peer"))
        || (message.startsWith("Failed to send RPC "));
  }

  @VisibleForTesting
  public TransportClientFactory getDataClientFactory() {
    return dataClientFactory;
  }

  class ReviveRequest {
    public int mapId;
    public int attemptId;
    public int partitionId;
    public int epoch;
    public PartitionLocation loc;
    public StatusCode cause;
    public volatile StatusCode reviveStatus;

    public ReviveRequest(
        int mapId,
        int attemptId,
        int partitionId,
        int epoch,
        PartitionLocation loc,
        StatusCode cause) {
      this.mapId = mapId;
      this.attemptId = attemptId;
      this.partitionId = partitionId;
      this.epoch = epoch;
      this.loc = loc;
      this.cause = cause;
      reviveStatus = StatusCode.REVIVE_INITIALIZED;
    }
  }

  class ReviveManager {
    // appid -> shuffleId -> requests
    ConcurrentHashMap<String, ConcurrentHashMap<Integer, Set<ReviveRequest>>> pendingRequests =
        new ConcurrentHashMap<>();
    ConcurrentHashMap<String, ConcurrentHashMap<Integer, Set<ReviveRequest>>> inProcessRequests =
        null;
    private ScheduledExecutorService reviveRequestScheduler =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler");

    public ReviveManager() {
      reviveRequestScheduler.scheduleAtFixedRate(
          new Runnable() {
            @Override
            public void run() {
              synchronized (this) {
                inProcessRequests = pendingRequests;
                pendingRequests = new ConcurrentHashMap<>();
              }
              if (!inProcessRequests.isEmpty()) {
                for (Map.Entry<String, ConcurrentHashMap<Integer, Set<ReviveRequest>>> entry :
                    inProcessRequests.entrySet()) {
                  String appId = entry.getKey();
                  ConcurrentHashMap<Integer, Set<ReviveRequest>> shuffleMap = entry.getValue();
                  for (Map.Entry<Integer, Set<ReviveRequest>> shuffleEntry :
                      shuffleMap.entrySet()) {
                    // Call reviveBatch for requests in the same (appId, shuffleId)
                    int shuffleId = shuffleEntry.getKey();
                    Set<ReviveRequest> requests = shuffleEntry.getValue();
                    int length = requests.size();
                    ArrayList<ReviveRequest> filteredRequests = new ArrayList<>(length);

                    // Insert request that is not MapperEnded and with the max epoch
                    // into filteredRequests
                    Iterator<ReviveRequest> iter = requests.iterator();
                    while (iter.hasNext()) {
                      ReviveRequest req = iter.next();
                      if (checkRevivedLocation(shuffleId, req.partitionId, req.epoch, false)) {
                        req.reviveStatus = StatusCode.SUCCESS;
                      } else if (mapperEnded(shuffleId, req.mapId, req.attemptId)) {
                        req.reviveStatus = StatusCode.MAP_ENDED;
                      } else {
                        filteredRequests.add(req);
                      }
                    }

                    if (!filteredRequests.isEmpty()) {
                      length = filteredRequests.size();
                      int[] mapIds = new int[length];
                      int[] attemptIds = new int[length];
                      int[] partitionIds = new int[length];
                      int[] epochs = new int[length];
                      StatusCode[] causes = new StatusCode[length];
                      PartitionLocation[] locs = new PartitionLocation[length];
                      for (int i = 0; i < length; i++) {
                        mapIds[i] = filteredRequests.get(i).mapId;
                        attemptIds[i] = filteredRequests.get(i).attemptId;
                        partitionIds[i] = filteredRequests.get(i).partitionId;
                        epochs[i] = filteredRequests.get(i).epoch;
                        causes[i] = filteredRequests.get(i).cause;
                        locs[i] = filteredRequests.get(i).loc;
                      }

                      // Call reviveBatch. Return null means Exception
                      Map<Integer, Map<Integer, Map<Integer, StatusCode>>> results =
                          reviveBatch(
                              appId,
                              shuffleId,
                              mapIds,
                              attemptIds,
                              partitionIds,
                              epochs,
                              locs,
                              causes);
                      if (results == null) {
                        for (ReviveRequest req : filteredRequests) {
                          req.reviveStatus = StatusCode.REVIVE_FAILED;
                        }
                      } else {
                        for (ReviveRequest req : filteredRequests) {
                          req.reviveStatus =
                              results.get(req.mapId).get(req.attemptId).get(req.partitionId);
                        }
                      }
                    } // End !filteredRequests.isEmpty()
                  } // End foreach shuffleMap.entrySet()
                } // End foreach inProcessRequests.entrySet()
              }

              inProcessRequests.clear();
            }
          },
          100,
          100,
          TimeUnit.MILLISECONDS);
    }

    public void addRequest(String appId, int shuffleId, ReviveRequest request) {
      blacklistByCause(request.cause, request.loc);
      synchronized (this) {
        ConcurrentHashMap<Integer, Set<ReviveRequest>> shuffleMap =
            pendingRequests.computeIfAbsent(appId, (id) -> new ConcurrentHashMap());
        Set<ReviveRequest> requests =
            shuffleMap.computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet());
        requests.add(request);
      }
    }
  }
}
