/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.server.rebalance;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.client.rebalance.RebalanceTaskInfo;
import voldemort.consistency.cluster.Cluster;
import voldemort.consistency.versioning.Version;
import voldemort.consistency.versioning.Versioned;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.server.rebalance.async.StealerBasedRebalanceAsyncOperation;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;
import voldemort.store.readonly.ReadOnlyStorageEngine;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Service responsible for rebalancing
 * 
 * <br>
 * 
 * Handles two scenarios a) When a new request comes in b) When a rebalancing
 * was shut down and the box was restarted
 * 
 */
public class Rebalancer implements Runnable {

    private final static Logger logger = Logger.getLogger(Rebalancer.class);

    private final MetadataStore metadataStore;
    private final AsyncOperationService asyncService;
    private final VoldemortConfig voldemortConfig;
    private final StoreRepository storeRepository;
    private final Set<Integer> rebalancePermits = Collections.synchronizedSet(new HashSet<Integer>());

    public Rebalancer(StoreRepository storeRepository,
                      MetadataStore metadataStore,
                      VoldemortConfig voldemortConfig,
                      AsyncOperationService asyncService) {
        this.storeRepository = storeRepository;
        this.metadataStore = metadataStore;
        this.asyncService = asyncService;
        this.voldemortConfig = voldemortConfig;
    }

    public AsyncOperationService getAsyncOperationService() {
        return asyncService;
    }

    public void start() {}

    public void stop() {}

    /**
     * This is called only once at startup
     */
    public void run() {}

    /**
     * Acquire a permit for a particular node id so as to allow rebalancing
     * 
     * @param nodeId The id of the node for which we are acquiring a permit
     * @return Returns true if permit acquired, false if the permit is already
     *         held by someone
     */
    public synchronized boolean acquireRebalancingPermit(int nodeId) {
        boolean added = rebalancePermits.add(nodeId);
        logger.info("Acquiring rebalancing permit for node id " + nodeId + ", returned: " + added);

        return added;
    }

    /**
     * Release the rebalancing permit for a particular node id
     * 
     * @param nodeId The node id whose permit we want to release
     */
    public synchronized void releaseRebalancingPermit(int nodeId) {
        boolean removed = rebalancePermits.remove(nodeId);
        logger.info("Releasing rebalancing permit for node id " + nodeId + ", returned: " + removed);
        if(!removed)
            throw new VoldemortException(new IllegalStateException("Invalid state, must hold a "
                                                                   + "permit to release"));
    }

    /**
     * Support four different stages <br>
     * For normal operation:
     * 
     * <pre>
     * | swapRO | changeClusterMetadata | changeRebalanceState | Order |
     * | f | t | t | rebalance -> cluster  | 
     * | f | f | t | rebalance |
     * | t | t | f | cluster -> swap |
     * | t | t | t | rebalance -> cluster -> swap|
     * </pre>
     * 
     * In general we need to do [ cluster change -> swap -> rebalance state
     * change ]
     * 
     * NOTE: The update of the cluster metadata and the rebalancer state is not
     * "atomic". Ergo, there could theoretically be a race where a client picks
     * up new cluster metadata sends a request based on that, but the proxy
     * bridges have not been setup and we either miss a proxy put or return a
     * null for get/getalls
     * 
     * TODO:refactor The rollback logic here is too convoluted. Specifically,
     * the independent updates to each key could be split up into their own
     * methods.
     * 
     * @param cluster Cluster metadata to change
     * @param rebalanceTaskInfo List of rebalance partitions info
     * @param swapRO Boolean to indicate swapping of RO store
     * @param changeClusterAndStoresMetadata Boolean to indicate a change of
     *        cluster metadata
     * @param changeRebalanceState Boolean to indicate a change in rebalance
     *        state
     * @param rollback Boolean to indicate that we are rolling back or not
     */
    public void rebalanceStateChange(Cluster cluster,
                                     List<StoreDefinition> storeDefs,
                                     List<RebalanceTaskInfo> rebalanceTaskInfo,
                                     boolean swapRO,
                                     boolean changeClusterAndStoresMetadata,
                                     boolean changeRebalanceState,
                                     boolean rollback) {
        Cluster currentCluster = metadataStore.getCluster();
        List<StoreDefinition> currentStoreDefs = metadataStore.getStoreDefList();

        logger.info("Server doing rebalance state change with options [ cluster metadata change - "
                    + changeClusterAndStoresMetadata + " ], [ changing rebalancing state - "
                    + changeRebalanceState + " ], [ changing swapping RO - " + swapRO
                    + " ], [ rollback - " + rollback + " ]");

        // Variables to track what has completed
        List<RebalanceTaskInfo> completedRebalanceTaskInfo = Lists.newArrayList();
        List<String> swappedStoreNames = Lists.newArrayList();
        boolean completedClusterAndStoresChange = false;
        boolean completedRebalanceSourceClusterChange = false;
        Cluster previousRebalancingSourceCluster = null;
        List<StoreDefinition> previousRebalancingSourceStores = null;

        try {

            /*
             * Do the rebalancing state changes. It is important that this
             * happens before the actual cluster metadata is changed. Here's
             * what could happen otherwise. When a batch completes with
             * {current_cluster c2, rebalancing_source_cluster c1} and the next
             * rebalancing state changes it to {current_cluster c3,
             * rebalancing_source_cluster c2} is set for the next batch, then
             * there could be a window during which the state is
             * {current_cluster c3, rebalancing_source_cluster c1}. On the other
             * hand, when we update the rebalancing source cluster first, there
             * is a window where the state is {current_cluster c2,
             * rebalancing_source_cluster c2}, which still fine, because of the
             * following. Successful completion of a batch means the cluster is
             * finalized, so its okay to stop proxying based on {current_cluster
             * c2, rebalancing_source_cluster c1}. And since the cluster
             * metadata has not yet been updated to c3, the writes will happen
             * based on c2.
             * 
             * 
             * Even if some clients have already seen the {current_cluster c3,
             * rebalancing_source_cluster c2} state from other servers, the
             * operation will be rejected with InvalidMetadataException since
             * this server itself is not aware of C3
             */

            // CHANGE REBALANCING STATE
            if(changeRebalanceState) {
                try {
                    previousRebalancingSourceCluster = metadataStore.getRebalancingSourceCluster();
                    previousRebalancingSourceStores = metadataStore.getRebalancingSourceStores();
                    if(!rollback) {

                        // Save up the current cluster and stores def for
                        // Redirecting store
                        changeClusterAndStores(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML,
                                               currentCluster,
                                               // Save the current store defs
                                               // for Redirecting store
                                               MetadataStore.REBALANCING_SOURCE_STORES_XML,
                                               currentStoreDefs);

                        completedRebalanceSourceClusterChange = true;

                        for(RebalanceTaskInfo info: rebalanceTaskInfo) {
                            metadataStore.addRebalancingState(info);
                            completedRebalanceTaskInfo.add(info);
                        }
                    } else {
                        // Reset the rebalancing source cluster back to null

                        changeClusterAndStores(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML, null,
                                               // Reset the rebalancing source
                                               // stores back to null
                                               MetadataStore.REBALANCING_SOURCE_STORES_XML,
                                               null);

                        completedRebalanceSourceClusterChange = true;

                        for(RebalanceTaskInfo info: rebalanceTaskInfo) {
                            metadataStore.deleteRebalancingState(info);
                            completedRebalanceTaskInfo.add(info);
                        }
                    }
                } catch(Exception e) {
                    throw new VoldemortException(e);
                }
            }

            // CHANGE CLUSTER METADATA AND STORE METADATA
            if(changeClusterAndStoresMetadata) {
                logger.info("Switching cluster metadata from " + currentCluster + " to " + cluster);
                logger.info("Switching stores metadata from " + currentStoreDefs + " to "
                            + storeDefs);
                changeClusterAndStores(MetadataStore.CLUSTER_KEY,
                                       cluster,
                                       MetadataStore.STORES_KEY,
                                       storeDefs);

                completedClusterAndStoresChange = true;

            }

            // SWAP RO DATA FOR ALL STORES
            if(swapRO) {
                swapROStores(swappedStoreNames, false);
            }

        } catch(VoldemortException e) {

            logger.error("Got exception while changing state, now rolling back changes", e);

            // ROLLBACK CLUSTER AND STORES CHANGE
            if(completedClusterAndStoresChange) {
                try {
                    logger.info("Rolling back cluster.xml to " + currentCluster);
                    logger.info("Rolling back stores.xml to " + currentStoreDefs);
                    changeClusterAndStores(MetadataStore.CLUSTER_KEY,
                                           currentCluster,
                                           MetadataStore.STORES_KEY,
                                           currentStoreDefs);
                } catch(Exception exception) {
                    logger.error("Error while rolling back cluster metadata to " + currentCluster
                                 + " Stores metadata to " + currentStoreDefs, exception);
                }
            }

            // SWAP RO DATA FOR ALL COMPLETED STORES
            if(swappedStoreNames.size() > 0) {
                try {
                    swapROStores(swappedStoreNames, true);
                } catch(Exception exception) {
                    logger.error("Error while swapping back to old state ", exception);
                }
            }

            // CHANGE BACK ALL REBALANCING STATES FOR COMPLETED ONES
            if(completedRebalanceTaskInfo.size() > 0) {
                if(!rollback) {
                    for(RebalanceTaskInfo info: completedRebalanceTaskInfo) {
                        try {
                            metadataStore.deleteRebalancingState(info);
                        } catch(Exception exception) {
                            logger.error("Error while deleting back rebalance info during error rollback "
                                                 + info,
                                         exception);
                        }
                    }
                } else {
                    for(RebalanceTaskInfo info: completedRebalanceTaskInfo) {
                        try {
                            metadataStore.addRebalancingState(info);
                        } catch(Exception exception) {
                            logger.error("Error while adding back rebalance info during error rollback "
                                                 + info,
                                         exception);
                        }
                    }
                }

            }

            // Revert changes to REBALANCING_SOURCE_CLUSTER_XML and
            // REBALANCING_SOURCE_STORES_XML
            if(completedRebalanceSourceClusterChange) {
                logger.info("Reverting the REBALANCING_SOURCE_CLUSTER_XML back to "
                            + previousRebalancingSourceCluster);
                logger.info("Reverting the REBALANCING_SOURCE_STORES_XML back to "
                            + previousRebalancingSourceStores);
                changeClusterAndStores(MetadataStore.REBALANCING_SOURCE_CLUSTER_XML,
                                       previousRebalancingSourceCluster,
                                       MetadataStore.REBALANCING_SOURCE_STORES_XML,
                                       previousRebalancingSourceStores);
            }

            throw e;
        }

    }

    /**
     * Goes through all the RO Stores in the plan and swaps it
     * 
     * @param swappedStoreNames Names of stores already swapped
     * @param useSwappedStoreNames Swap only the previously swapped stores (
     *        Happens during error )
     */
    private void swapROStores(List<String> swappedStoreNames, boolean useSwappedStoreNames) {

        try {
            for(StoreDefinition storeDef: metadataStore.getStoreDefList()) {

                // Only pick up the RO stores
                if(storeDef.getType().compareTo(ReadOnlyStorageConfiguration.TYPE_NAME) == 0) {

                    if(useSwappedStoreNames && !swappedStoreNames.contains(storeDef.getName())) {
                        continue;
                    }

                    ReadOnlyStorageEngine engine = (ReadOnlyStorageEngine) storeRepository.getStorageEngine(storeDef.getName());

                    if(engine == null) {
                        throw new VoldemortException("Could not find storage engine for "
                                                     + storeDef.getName() + " to swap ");
                    }

                    logger.info("Swapping RO store " + storeDef.getName());

                    // Time to swap this store - Could have used admin client,
                    // but why incur the overhead?
                    engine.swapFiles(engine.getCurrentDirPath());

                    // Add to list of stores already swapped
                    if(!useSwappedStoreNames)
                        swappedStoreNames.add(storeDef.getName());
                }
            }
        } catch(Exception e) {
            logger.error("Error while swapping RO store");
            throw new VoldemortException(e);
        }
    }

    /**
     * Updates the cluster and store metadata atomically
     * 
     * This is required during rebalance and expansion into a new zone since we
     * have to update the store def along with the cluster def.
     * 
     * @param cluster The cluster metadata information
     * @param storeDefs The stores metadata information
     */
    private void changeClusterAndStores(String clusterKey,
                                        final Cluster cluster,
                                        String storesKey,
                                        final List<StoreDefinition> storeDefs) {
        metadataStore.writeLock.lock();
        try {
            Version updatedVectorClock = ( metadataStore.get(clusterKey, null)
                                                                         .get(0)
                                                                         .getVersion()).incremented(metadataStore.getNodeId(),
                                                                                                    System.currentTimeMillis());
            metadataStore.put(clusterKey, Versioned.value((Object) cluster, updatedVectorClock));

            // now put new stores
            updatedVectorClock = (metadataStore.get(storesKey, null)
                                                             .get(0)
                                                             .getVersion()).incremented(metadataStore.getNodeId(),
                                                                                        System.currentTimeMillis());
            metadataStore.put(storesKey, Versioned.value((Object) storeDefs, updatedVectorClock));

        } catch(Exception e) {
            logger.info("Error while changing cluster to " + cluster + "for key " + clusterKey);
            throw new VoldemortException(e);
        } finally {
            metadataStore.writeLock.unlock();
        }
    }

    /**
     * This function is responsible for starting the actual async rebalance
     * operation. This is run if this node is the stealer node
     * 
     * <br>
     * 
     * We also assume that the check that this server is in rebalancing state
     * has been done at a higher level
     * 
     * @param stealInfo Partition info to steal
     * @return Returns a id identifying the async operation
     */
    public int rebalanceNode(final RebalanceTaskInfo stealInfo) {

        final RebalanceTaskInfo info = metadataStore.getRebalancerState()
                                                    .find(stealInfo.getDonorId());

        // Do we have the plan in the state?
        if(info == null) {
            throw new VoldemortException("Could not find plan " + stealInfo
                                         + " in the server state on " + metadataStore.getNodeId());
        } else if(!info.equals(stealInfo)) {
            // If we do have the plan, is it the same
            throw new VoldemortException("The plan in server state " + info
                                         + " is not the same as the process passed " + stealInfo);
        } else if(!acquireRebalancingPermit(stealInfo.getDonorId())) {
            // Both are same, now try to acquire a lock for the donor node
            throw new AlreadyRebalancingException("Node " + metadataStore.getNodeId()
                                                  + " is already rebalancing from donor "
                                                  + info.getDonorId() + " with info " + info);
        }

        // Acquired lock successfully, start rebalancing...
        int requestId = asyncService.getUniqueRequestId();

        // Why do we pass 'info' instead of 'stealInfo'? So that we can change
        // the state as the stores finish rebalance
        asyncService.submitOperation(requestId,
                                     new StealerBasedRebalanceAsyncOperation(this,
                                                                             voldemortConfig,
                                                                             metadataStore,
                                                                             requestId,
                                                                             info));

        return requestId;
    }
}
