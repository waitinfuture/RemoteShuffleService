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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.ReviveRequest;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReviveManager {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleClientImpl.class);

  // appId -> (shuffleId -> requests)
  HashMap<String, HashMap<Integer, Set<ReviveRequest>>> pendingRequests = new HashMap<>();
  HashMap<String, HashMap<Integer, Set<ReviveRequest>>> inProcessRequests = null;
  ShuffleClientImpl shuffleClient;
  private ScheduledExecutorService batchReviveRequestScheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler");

  public ReviveManager(ShuffleClientImpl shuffleClient) {
    this.shuffleClient = shuffleClient;

    batchReviveRequestScheduler.scheduleAtFixedRate(
        new Runnable() {
          @Override
          public void run() {
            // We must ensure the swap is atomic
            synchronized (this) {
              inProcessRequests = pendingRequests;
              pendingRequests = new HashMap<>();
            }
            if (!inProcessRequests.isEmpty()) {
              for (Map.Entry<String, HashMap<Integer, Set<ReviveRequest>>> entry :
                  inProcessRequests.entrySet()) {
                String appId = entry.getKey();
                HashMap<Integer, Set<ReviveRequest>> shuffleMap = entry.getValue();
                for (Map.Entry<Integer, Set<ReviveRequest>> shuffleEntry : shuffleMap.entrySet()) {
                  // Call reviveBatch for requests in the same (appId, shuffleId)
                  int shuffleId = shuffleEntry.getKey();
                  Set<ReviveRequest> requests = shuffleEntry.getValue();
                  Set<Integer> mapIds = new HashSet<>();
                  ArrayList<ReviveRequest> filteredRequests = new ArrayList<>();
                  Map<Integer, ReviveRequest> requestsToSend = new HashMap<>();

                  // Insert request that is not MapperEnded and with the max epoch
                  // into requestsToSend
                  Iterator<ReviveRequest> iter = requests.iterator();
                  while (iter.hasNext()) {
                    ReviveRequest req = iter.next();
                    if (shuffleClient.checkRevivedLocation(
                        shuffleId, req.partitionId, req.epoch, false) || shuffleClient.mapperEnded(shuffleId, req.mapId)) {
                      req.reviveStatus = StatusCode.SUCCESS.getValue();
                    } else {
                      filteredRequests.add(req);
                      mapIds.add(req.mapId);
                      PartitionLocation loc = req.loc;
                      if (!requestsToSend.containsKey(loc.getId())
                          || requestsToSend.get(loc.getId()).epoch < req.epoch) {
                        requestsToSend.put(loc.getId(), req);
                      }
                    }
                  }

                  if (!requestsToSend.isEmpty()) {
                    // Call reviveBatch. Return null means Exception caught or
                    // SHUFFLE_NOT_REGISTERED
                    long startTime = System.currentTimeMillis();
                    Map<Integer, Integer> results =
                        shuffleClient.reviveBatch(
                            appId, shuffleId, mapIds, requestsToSend.values());
                    logger.info("ReviveBatch costs {} ms", System.currentTimeMillis() - startTime);
                    if (results == null) {
                      for (ReviveRequest req : filteredRequests) {
                        req.reviveStatus = StatusCode.REVIVE_FAILED.getValue();
                      }
                    } else {
                      for (ReviveRequest req : filteredRequests) {
                        if (shuffleClient.mapperEnded(shuffleId, req.mapId)) {
                          req.reviveStatus = StatusCode.SUCCESS.getValue();
                        } else {
                          req.reviveStatus = results.get(req.partitionId);
                        }
                      }
                    }
                  }
                } // End foreach shuffleMap.entrySet
              } // End foreach inProcessRequests.entrySet
            }

            inProcessRequests.clear();
          }
        },
        100,
        100,
        TimeUnit.MILLISECONDS);
  }

  public void addRequest(String appId, int shuffleId, ReviveRequest request) {
    shuffleClient.excludeWorkerByCause(request.cause, request.loc);
    // This sync is necessary to ensure the add action is atomic
    synchronized (this) {
      HashMap<Integer, Set<ReviveRequest>> shuffleMap =
          pendingRequests.computeIfAbsent(appId, (id) -> new HashMap());
      Set<ReviveRequest> requests =
          shuffleMap.computeIfAbsent(shuffleId, (id) -> ConcurrentHashMap.newKeySet());
      requests.add(request);
    }
  }
}
