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

package voldemort.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.VoldemortException;
import voldemort.consistency.cluster.Cluster;
import voldemort.consistency.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.Pair;
import voldemort.utils.Utils;

import com.google.common.collect.Lists;

/**
 * This class wraps up a Cluster object and a StoreDefinition. The methods are
 * effectively helper or util style methods for querying the routing plan that
 * will be generated for a given routing strategy upon store and cluster
 * topology information.
 * 
 * This object can be expensive to construct. Invocations of getters should be
 * O(1) if possible. Lazy initialization of cached values is ideal...
 * 
 * The intermingling of key-based interfaces and partition-based interfaces is
 * ugly in this class. Partition-based interfaces should be in an underlying
 * class and then key-based interfaces should wrap those up. Or, all of these
 * types of class should implement the getMasterPartition(byte[]) interface from
 * RoutingStrategy. Then the caller always does translation from key to
 * partition ID, and the *StoreRouting* classes only offer partition-based
 * interfaces.
 */
// TODO: Should find a better name for this class. Need to do same for what is
// currently BaseStoreRoutingPlan. Suggestions thus far for this class:
// RoutingInfoHelper, ComplicatedStoreRoutingPlan, ClusterStoreRouter,
// ClusterStoreRoutingPlan. Ideally, the name makes this classes use case
// (expensive construction, fast lookups) clear, and distinct from the use case
// for what is now the BaseStoreRoutingPlan.

// TODO: Review all objects that construct a StoreRoutingPlan to see if the
// usage would be better served by BaseStoreRoutingPlan. The former is expensive
// to construct (many ms) whereas the latter is cheap to construct (us).
public class StoreRoutingPlan {

    private final Cluster cluster;
    private final StoreDefinition storeDefinition;
    private final RoutingStrategy routingStrategy;

    private final Node[] partitionToNode;
    private final ArrayList<List<Integer>> partitionIdToReplicatingList;

    private final Map<Integer, List<Integer>> nodeIdToNaryPartitionMap;
    private final Map<Integer, List<Integer>> nodeIdToZonePrimaryMap;

    public StoreRoutingPlan(Cluster cluster, StoreDefinition storeDefinition) {
        this.cluster = cluster;
        this.storeDefinition = storeDefinition;
        this.routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition,
                                                                                  cluster);
        this.partitionToNode = cluster.getPartitionIdToNodeArray();

        this.partitionIdToReplicatingList = new ArrayList<List<Integer>>(this.partitionToNode.length);
        for(int partitionId = 0; partitionId < partitionToNode.length; ++partitionId) {
            this.partitionIdToReplicatingList.add(partitionId,
                                                  routingStrategy.getReplicatingPartitionList(partitionId));
        }

        verifyClusterStoreDefinition();

        this.nodeIdToNaryPartitionMap = new HashMap<Integer, List<Integer>>();
        this.nodeIdToZonePrimaryMap = new HashMap<Integer, List<Integer>>();
        for(int nodeId: cluster.getNodeIds()) {
            this.nodeIdToNaryPartitionMap.put(nodeId, new ArrayList<Integer>());
            this.nodeIdToZonePrimaryMap.put(nodeId, new ArrayList<Integer>());
        }
        for(int masterPartitionId = 0; masterPartitionId < cluster.getNumberOfPartitions(); ++masterPartitionId) {
            List<Integer> naryPartitionIds = getReplicatingPartitionList(masterPartitionId);
            for(int naryPartitionId: naryPartitionIds) {
                int naryNodeId = getNodeIdForPartitionId(naryPartitionId);
                this.nodeIdToNaryPartitionMap.get(naryNodeId).add(masterPartitionId);
            }
        }
        for(int nodeId: cluster.getNodeIds()) {
            int naryZoneId = cluster.getNodeById(nodeId).getZoneId();
            List<Integer> naryPartitionIds = this.nodeIdToNaryPartitionMap.get(nodeId);
            List<Integer> zoneNAries = this.nodeIdToZonePrimaryMap.get(nodeId);
            for(int naryPartitionId: naryPartitionIds) {
                if(getZoneNaryForNodesPartition(naryZoneId, nodeId, naryPartitionId) == 0) {
                    zoneNAries.add(naryPartitionId);
                }
            }
        }

    }

    public Cluster getCluster() {
        return cluster;
    }

    public StoreDefinition getStoreDefinition() {
        return storeDefinition;
    }

    /**
     * Verify that cluster is congruent to store def wrt zones.
     */
    private void verifyClusterStoreDefinition() {
        if(SystemStoreConstants.isSystemStore(storeDefinition.getName())) {
            // TODO: Once "todo" in StorageService.initSystemStores is complete,
            // this early return can be removed and verification can be enabled
            // for system stores.
            return;
        }

        Set<Integer> clusterZoneIds = cluster.getZoneIds();

        if(clusterZoneIds.size() > 1) { // Zoned
            Map<Integer, Integer> zoneRepFactor = storeDefinition.getZoneReplicationFactor();
            Set<Integer> storeDefZoneIds = zoneRepFactor.keySet();

            if(!clusterZoneIds.equals(storeDefZoneIds)) {
                throw new VoldemortException("Zone IDs in cluster (" + clusterZoneIds
                                             + ") are incongruent with zone IDs in store defs ("
                                             + storeDefZoneIds + ")");
            }

            for(int zoneId: clusterZoneIds) {
                if(zoneRepFactor.get(zoneId) > cluster.getNumberOfNodesInZone(zoneId)) {
                    throw new VoldemortException("Not enough nodes ("
                                                 + cluster.getNumberOfNodesInZone(zoneId)
                                                 + ") in zone with id " + zoneId
                                                 + " for replication factor of "
                                                 + zoneRepFactor.get(zoneId) + ".");
                }
            }
        } else { // Non-zoned

            if(storeDefinition.getReplicationFactor() > cluster.getNumberOfNodes()) {
                System.err.println(storeDefinition);
                System.err.println(cluster);
                throw new VoldemortException("Not enough nodes (" + cluster.getNumberOfNodes()
                                             + ") for replication factor of "
                                             + storeDefinition.getReplicationFactor() + ".");
            }
        }
    }

    /**
     * Determines list of partition IDs that replicate the master partition ID.
     * 
     * @param masterPartitionId
     * @return List of partition IDs that replicate the master partition ID.
     */
    public List<Integer> getReplicatingPartitionList(int masterPartitionId) {
        return partitionIdToReplicatingList.get(masterPartitionId);
    }

    /**
     * Determines list of partition IDs that replicate the key.
     * 
     * @param key
     * @return List of partition IDs that replicate the given key
     */
    public List<Integer> getReplicatingPartitionList(final byte[] key) {
        return getReplicatingPartitionList(this.routingStrategy.getMasterPartition(key));
    }

    /**
     * Determines master partition ID for the key.
     * 
     * @param key
     * @return master partition id
     */
    public int getMasterPartitionId(final byte[] key) {
        return this.routingStrategy.getMasterPartition(key);
    }

    /**
     * Determines node ID that hosts the specified partition ID.
     * 
     * @param partitionId
     * @return node id that hosts primary partition id.
     */
    public int getNodeIdForPartitionId(int partitionId) {
        return partitionToNode[partitionId].getId();
    }

    /**
     * Determines the partition ID that replicates the key on the given node.
     * 
     * @param nodeId of the node
     * @param key to look up.
     * @return partitionId if found, otherwise null.
     */
    public Integer getNodesPartitionIdForKey(int nodeId, final byte[] key) {
        // this is all the partitions the key replicates to.
        List<Integer> partitionIds = getReplicatingPartitionList(key);
        for(Integer partitionId: partitionIds) {
            // check which of the replicating partitions belongs to the node in
            // question
            if(getNodeIdForPartitionId(partitionId) == nodeId) {
                return partitionId;
            }
        }
        return null;
    }

    /**
     * Converts from partitionId to nodeId. The list of partition IDs,
     * partitionIds, is expected to be a "replicating partition list", i.e., the
     * mapping from partition ID to node ID should be one to one.
     * 
     * @param partitionIds List of partition IDs for which to find the Node ID
     *        for the Node that owns the partition.
     * @return List of node ids, one for each partition ID in partitionIds
     * @throws VoldemortException If multiple partition IDs in partitionIds map
     *         to the same Node ID.
     */
    private List<Integer> getNodeIdListForPartitionIdList(List<Integer> partitionIds)
            throws VoldemortException {
        List<Integer> nodeIds = new ArrayList<Integer>(partitionIds.size());
        for(Integer partitionId: partitionIds) {
            int nodeId = getNodeIdForPartitionId(partitionId);
            if(nodeIds.contains(nodeId)) {
                throw new VoldemortException("Node ID " + nodeId + " already in list of Node IDs.");
            } else {
                nodeIds.add(nodeId);
            }
        }
        return nodeIds;
    }

    /**
     * Returns the list of node ids this partition replicates to.
     * 
     * TODO ideally the {@link RoutingStrategy} should house a routeRequest(int
     * partition) method
     * 
     * @param partitionId
     * @return list of node ids
     * @throws VoldemortException
     */
    public List<Integer> getReplicationNodeList(int partitionId) throws VoldemortException {
        return getNodeIdListForPartitionIdList(getReplicatingPartitionList(partitionId));
    }

    /**
     * Returns all (zone n-ary) partition IDs hosted on the node.
     * 
     * @param nodeId
     * @return all zone n-ary partition IDs hosted on the node in an unordered
     *         list.
     */
    public List<Integer> getZoneNAryPartitionIds(int nodeId) {
        return nodeIdToNaryPartitionMap.get(nodeId);
    }

    /**
     * Returns all zone-primary partition IDs on node. A zone-primary means zone
     * n-ary==0. Zone-primary nodes are generally pseudo-masters in the zone and
     * receive get traffic for some partition Id.
     * 
     * @param nodeId
     * @return all primary partition IDs (zone n-ary == 0) hosted on the node.
     */
    public List<Integer> getZonePrimaryPartitionIds(int nodeId) {
        return nodeIdToZonePrimaryMap.get(nodeId);
    }

    /**
     * checks if zone has a zone n-ary for partition Id. False should only be
     * returned in zone expansion use cases.
     * 
     * @param zoneId
     * @param zoneNAry zone n-ary replica to confirm
     * @param partitionId
     * @return true iff partitionId has zone-nary replica in zone id .
     */
    // TODO: add unit test.
    public boolean zoneNAryExists(int zoneId, int zoneNAry, int partitionId) {
        int currentZoneNAry = -1;
        for(int replicatingNodeId: getReplicationNodeList(partitionId)) {
            Node replicatingNode = cluster.getNodeById(replicatingNodeId);
            if(replicatingNode.getZoneId() == zoneId) {
                currentZoneNAry++;
                if(currentZoneNAry == zoneNAry) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Determines the zone n-ary replica level of the specified partitionId on
     * the node id in zone id.
     * 
     * @param zoneId
     * @param nodeId
     * @param partitionId
     * @return zone n-ary replica level of the partition id on the node id in
     *         the zone id (primary == 0, secondary == 1, ...)
     */
    // TODO: add unit test.
    public int getZoneNaryForNodesPartition(int zoneId, int nodeId, int partitionId) {
        if(cluster.getNodeById(nodeId).getZoneId() != zoneId) {
            throw new VoldemortException("Node " + nodeId + " is not in zone " + zoneId
                                         + "! The node is in zone "
                                         + cluster.getNodeById(nodeId).getZoneId());
        }

        List<Integer> replicatingNodeIds = getReplicationNodeList(partitionId);
        int zoneNAry = -1;
        for(int replicatingNodeId: replicatingNodeIds) {
            Node replicatingNode = cluster.getNodeById(replicatingNodeId);
            // bump up the replica number once you encounter a node in the given
            // zone
            if(replicatingNode.getZoneId() == zoneId) {
                zoneNAry++;
            }
            if(replicatingNodeId == nodeId) {
                return zoneNAry;
            }
        }
        if(zoneNAry > 0) {
            throw new VoldemortException("Node " + nodeId + " not a replica for partition "
                                         + partitionId + " in given zone " + zoneId);
        } else {
            throw new VoldemortException("Could not find any replicas for partition Id "
                                         + partitionId + " in given zone " + zoneId);
        }
    }

    /**
     * Determines if the key replicates to the given node
     * 
     * @param key
     * @param nodeId
     * @return true if the key belongs to the node as some replica
     */
    public boolean checkKeyBelongsToNode(byte[] key, int nodeId) {
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = getReplicatingPartitionList(key);
        // remove all partitions from the list, except those that belong to the
        // node
        replicatingPartitions.retainAll(nodePartitions);
        return replicatingPartitions.size() > 0;
    }

    // TODO: (refactor) Move from static methods to non-static methods that use
    // this object's cluster and storeDefinition member for the various
    // check*BelongsTo* methods. Also, tweak internal members to make these
    // checks easier/faster.
    /**
     * Check that the key belongs to one of the partitions in the map of replica
     * type to partitions
     * 
     * @param keyPartitions Preference list of the key
     * @param nodePartitions Partition list on this node
     * @param replicaToPartitionList Mapping of replica type to partition list
     * @return Returns a boolean to indicate if this belongs to the map
     */
    public static boolean checkKeyBelongsToPartition(List<Integer> keyPartitions,
                                                     List<Integer> nodePartitions,
                                                     HashMap<Integer, List<Integer>> replicaToPartitionList) {
        // Check for null
        replicaToPartitionList = Utils.notNull(replicaToPartitionList);

        for(int replicaNum = 0; replicaNum < keyPartitions.size(); replicaNum++) {

            // If this partition belongs to node partitions + master is in
            // replicaToPartitions list -> match
            if(nodePartitions.contains(keyPartitions.get(replicaNum))) {
                List<Integer> partitionsToMove = replicaToPartitionList.get(replicaNum);
                if(partitionsToMove != null && partitionsToMove.size() > 0) {
                    if(partitionsToMove.contains(keyPartitions.get(0))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Check that the key belongs to one of the partitions in the map of replica
     * type to partitions
     * 
     * @param nodeId Node on which this is running ( generally stealer node )
     * @param key The key to check
     * @param replicaToPartitionList Mapping of replica type to partition list
     * @param cluster Cluster metadata
     * @param storeDef The store definition
     * @return Returns a boolean to indicate if this belongs to the map
     */
    public static boolean checkKeyBelongsToPartition(int nodeId,
                                                     byte[] key,
                                                     HashMap<Integer, List<Integer>> replicaToPartitionList,
                                                     Cluster cluster,
                                                     StoreDefinition storeDef) {
        boolean checkResult = false;
        if(storeDef.getRoutingStrategyType().equals(RoutingStrategyType.TO_ALL_STRATEGY)
           || storeDef.getRoutingStrategyType()
                      .equals(RoutingStrategyType.TO_ALL_LOCAL_PREF_STRATEGY)) {
            checkResult = true;
        } else {
            List<Integer> keyPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                             cluster)
                                                                      .getPartitionList(key);
            List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
            checkResult = StoreRoutingPlan.checkKeyBelongsToPartition(keyPartitions,
                                                                      nodePartitions,
                                                                      replicaToPartitionList);
        }
        return checkResult;
    }

    /**
     * Given a key and a list of steal infos give back a list of stealer node
     * ids which will steal this.
     * 
     * @param key Byte array of key
     * @param stealerNodeToMappingTuples Pairs of stealer node id to their
     *        corresponding [ partition - replica ] tuples
     * @param cluster Cluster metadata
     * @param storeDef Store definitions
     * @return List of node ids
     */
    public static List<Integer> checkKeyBelongsToPartition(byte[] key,
                                                           Set<Pair<Integer, HashMap<Integer, List<Integer>>>> stealerNodeToMappingTuples,
                                                           Cluster cluster,
                                                           StoreDefinition storeDef) {
        List<Integer> keyPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                         cluster)
                                                                  .getPartitionList(key);
        List<Integer> nodesToPush = Lists.newArrayList();
        for(Pair<Integer, HashMap<Integer, List<Integer>>> stealNodeToMap: stealerNodeToMappingTuples) {
            List<Integer> nodePartitions = cluster.getNodeById(stealNodeToMap.getFirst())
                                                  .getPartitionIds();
            if(StoreRoutingPlan.checkKeyBelongsToPartition(keyPartitions,
                                                           nodePartitions,
                                                           stealNodeToMap.getSecond())) {
                nodesToPush.add(stealNodeToMap.getFirst());
            }
        }
        return nodesToPush;
    }

    /***
     * Checks if a given partition is stored in the node.
     * 
     * @param partition
     * @param nodeId
     * @param cluster
     * @param storeDef
     * @return true if partition belongs to node for given store
     */
    public static boolean checkPartitionBelongsToNode(int partition,
                                                      int nodeId,
                                                      Cluster cluster,
                                                      StoreDefinition storeDef) {
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getReplicatingPartitionList(partition);
        replicatingPartitions.retainAll(nodePartitions);
        return replicatingPartitions.size() > 0;
    }

    /**
     * 
     * @param key
     * @param nodeId
     * @param cluster
     * @param storeDef
     * @return true if the key belongs to the node as some replica
     */
    public static boolean checkKeyBelongsToNode(byte[] key,
                                                int nodeId,
                                                Cluster cluster,
                                                StoreDefinition storeDef) {
        List<Integer> nodePartitions = cluster.getNodeById(nodeId).getPartitionIds();
        List<Integer> replicatingPartitions = new RoutingStrategyFactory().updateRoutingStrategy(storeDef,
                                                                                                 cluster)
                                                                          .getPartitionList(key);
        // remove all partitions from the list, except those that belong to the
        // node
        replicatingPartitions.retainAll(nodePartitions);
        return replicatingPartitions.size() > 0;
    }

    /**
     * Determines which node hosts partition id with specified n-ary level in
     * specified zone.
     * 
     * @param zoneId
     * @param zoneNary
     * @param partitionId
     * @return node ID that hosts zone n-ary replica of partition.
     */
    // TODO: add unit test.
    public int getNodeIdForZoneNary(int zoneId, int zoneNary, int partitionId) {
        List<Integer> replicatingNodeIds = getReplicationNodeList(partitionId);

        int zoneNAry = -1;
        for(int replicatingNodeId: replicatingNodeIds) {
            Node replicatingNode = cluster.getNodeById(replicatingNodeId);
            // bump up the counter if we encounter a replica in the given zone
            if(replicatingNode.getZoneId() == zoneId) {
                zoneNAry++;
            }
            // when the counter matches up with the replicaNumber we need, we
            // are done.
            if(zoneNAry == zoneNary) {
                return replicatingNode.getId();
            }
        }
        if(zoneNAry == 0) {
            throw new VoldemortException("Could not find any replicas for the partition "
                                         + partitionId + " in given zone " + zoneId);
        } else {
            throw new VoldemortException("Could not find " + zoneNary
                                         + " replicas for the partition " + partitionId
                                         + " in given zone " + zoneId + ". Only found " + zoneNAry);
        }
    }

}
