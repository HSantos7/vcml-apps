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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import voldemort.client.protocol.pb.VAdminProto.FetchPartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StreamingStats;
import voldemort.utils.NetworkClassLoader;

/**
 * Base class for key/entry stream fetching handlers that use efficient
 * partition scan (PID layout). Of course, only works if
 * isPartitionScanSupported() is true for the storage engine to be scanned..
 * 
 */
public abstract class PartitionScanFetchStreamRequestHandler extends FetchStreamRequestHandler {

    protected Set<Integer> fetchedPartitions;
    protected List<Integer> partitionList;

    protected Integer currentIndex;
    protected Integer currentPartition;
    protected long currentPartitionFetched;

    public PartitionScanFetchStreamRequestHandler(FetchPartitionEntriesRequest request,
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

        fetchedPartitions = new HashSet<Integer>();
        partitionList = new ArrayList<Integer>();

        partitionList = partitionIds;
        currentIndex = 0;
        currentPartition = null;
        currentPartitionFetched = 0;
    }

    /**
     * Simple info message for status
     * 
     * @param tag Message to print out at start of info message
     */
    protected void statusInfoMessage(final String tag) {
        if(logger.isInfoEnabled()) {
            logger.info(tag + " : [partition: " + currentPartition + ", partitionFetched: "
                        + currentPartitionFetched
                        + "] for store " + storageEngine.getName());
        }
    }

    /**
     * True iff enough items have been fetched for current partition
     * 
     * @return true iff enough items have been fetched for current partition.
     */
    protected boolean fetchedEnoughForCurrentPartition() {
        if(recordsPerPartition <= 0) {
            return false;
        }
        return (currentPartitionFetched >= recordsPerPartition);
    }

    /**
     * Account for fetch.
     * 
     */
    protected void recordFetched() {
        fetched++;
        currentPartitionFetched++;
        if(streamStats != null) {
            streamStats.reportStreamingFetch(operation);
        }
    }

    /**
     * Called when current partition has been completely fetched.
     */
    protected void completedFetchingCurrentPartition() {
        fetchedPartitions.add(currentPartition);
        currentPartitionFetched = 0;
    }
}
