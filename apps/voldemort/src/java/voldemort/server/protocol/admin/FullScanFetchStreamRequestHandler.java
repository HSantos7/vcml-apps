/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.server.protocol.admin;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import voldemort.client.protocol.pb.VAdminProto.FetchPartitionEntriesRequest;
import voldemort.routing.StoreRoutingPlan;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StreamingStats;
import voldemort.consistency.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Utils;

/**
 * Base class for key/entry stream fetching handlers that do an unordered full scan to fetch items.
 * 
 */
public abstract class FullScanFetchStreamRequestHandler extends FetchStreamRequestHandler {

    protected final ClosableIterator<ByteArray> keyIterator;

    // PartitionId to count of fetches on that partition.
    protected Map<Integer, Long> partitionFetches;

    // PartitionIds of partitions that still need more fetched...
    protected Set<Integer> partitionsToFetch;

    public FullScanFetchStreamRequestHandler(FetchPartitionEntriesRequest request,
                                             MetadataStore metadataStore,
                                             ErrorCodeMapper errorCodeMapper,
                                             VoldemortConfig voldemortConfig,
                                             StoreRepository storeRepository,
                                             NetworkClassLoader networkClassLoader,
                                             StreamingStats.Operation operation) {
        super(request,
              metadataStore,
              errorCodeMapper,
              voldemortConfig,
              storeRepository,
              networkClassLoader,
                operation);
        this.keyIterator = storageEngine.keys();
        this.partitionFetches = new HashMap<Integer, Long>();
        for (Integer partitionId : partitionIds) {
            this.partitionFetches.put(partitionId, new Long(0));
        }
        this.partitionsToFetch = new HashSet<Integer>(partitionIds);
    }

    /**
     * Given the key, figures out which partition on the local node hosts the key.
     * 
     * @param key
     * @return
     */
    private Integer getKeyPartitionId(byte[] key) {
        Integer keyPartitionId = storeInstance.getNodesPartitionIdForKey(nodeId, key);

        Utils.notNull(keyPartitionId);
        return keyPartitionId;
    }

    /**
     * Given the key, figures out the master partition id
     * 
     * @param key
     * @return
     */
    private Integer getMasterPartitionId(byte[] key) {
        return storeInstance.getMasterPartitionId(key);
    }

    /**
     * Determines if the key is needed. To be 'needed', a key must (i) belong to a partition being
     * requested and (ii) be necessary to meet recordsPerPartition constraint, if any.
     * 
     * @param key
     * @return true iff key is needed.
     */
    protected boolean isKeyNeeded(byte[] key) {
        // Empty partitions list has a special meaning in that all keys should be returned
        if (partitionsToFetch.isEmpty()) {
            return true;
        }
        // Exit early if the master partition corresponding to the key is not in the toFetch list
        if (!partitionsToFetch.contains(getMasterPartitionId(key))) {
            return false;
        }
        if (recordsPerPartition <= 0) {
            return true;
        }
        return true;
    }

    /**
     * Determines if entry is accepted. For normal usage, this means confirming that the key is
     * needed. For orphan usage, this simply means confirming the key belongs to the node.
     * 
     * @param key
     * @return true iff entry is accepted.
     */
    protected boolean isItemAccepted(byte[] key) {
        boolean entryAccepted = false;
        if (!fetchOrphaned) {
            if (isKeyNeeded(key)) {
                entryAccepted = true;
            }
        } else {
            if (!StoreRoutingPlan.checkKeyBelongsToNode(key, nodeId, initialCluster, storeDef)) {
                entryAccepted = true;
            }
        }
        return entryAccepted;
    }

    /**
     * Account for key being fetched.
     * 
     * @param key
     */
    protected void accountForFetchedKey(byte[] key) {
        fetched++;
        if (streamStats != null) {
            streamStats.reportStreamingFetch(operation);
        }

        if (recordsPerPartition <= 0) {
            return;
        }

        Integer keyPartitionId = getKeyPartitionId(key);
        Long partitionFetch = partitionFetches.get(keyPartitionId);
        Utils.notNull(partitionFetch);
        partitionFetch++;

        partitionFetches.put(keyPartitionId, partitionFetch);
        if (partitionFetch == recordsPerPartition) {
            if (partitionsToFetch.contains(keyPartitionId)) {
                partitionsToFetch.remove(keyPartitionId);
            } else {
                logger.warn("Partitions to fetch did not contain expected partition ID: "
                        + keyPartitionId);
            }
        } else if (partitionFetch > recordsPerPartition) {
            logger.warn("Partition fetch count larger than expected for partition ID "
                    + keyPartitionId + " : " + partitionFetch);
        }
    }

    /**
     * True iff enough items have been fetched for all partitions, where 'enough' is relative to
     * recordsPerPartition value.
     * 
     * @return true iff enough items have been fetched for all partitions
     */
    protected boolean fetchedEnoughForAllPartitions() {
        if (recordsPerPartition <= 0) {
            return false;
        }

        if (partitionsToFetch.size() > 0) {
            return false;
        }
        return true;
    }

    /**
     * Determines if still WRITING or COMPLETE.
     * 
     * @param itemTag mad libs style string to insert into progress message.
     * @return state of stream request handler
     */
    protected StreamRequestHandlerState determineRequestHandlerState(String itemTag) {

        if (keyIterator.hasNext() && !fetchedEnoughForAllPartitions()) {
            return StreamRequestHandlerState.WRITING;
        } else {
            logger.info("Finished fetch " + itemTag + " for store '" + storageEngine.getName()
                    + "' with partitions  " + partitionIds);
            progressInfoMessage("Fetch " + itemTag + " (end of scan)");

            return StreamRequestHandlerState.COMPLETE;
        }
    }

    @Override
    public final void close(DataOutputStream outputStream) throws IOException {
        if (null != keyIterator)
            keyIterator.close();
        super.close(outputStream);
    }
}
