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
package voldemort.client.rebalance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import voldemort.ClusterTestUtils;
import voldemort.consistency.cluster.Cluster;
import voldemort.store.StoreDefinition;

public class ZonedRebalanceBatchPlanTest {

    static Cluster zzCurrent;
    static Cluster zzShuffle;
    static Cluster zzClusterExpansionNN;
    static Cluster zzClusterExpansionPP;
    static List<StoreDefinition> zzStores;

    static Cluster zzzCurrent;
    static Cluster zzzShuffle;
    static Cluster zzzClusterExpansionNNN;
    static Cluster zzzClusterExpansionPPP;
    static Cluster zzeZoneExpansion;
    static Cluster zzzZoneExpansionXXP;
    static List<StoreDefinition> zzzStores;
    
    static Cluster z1z3Current;
    static Cluster z1z3z5Current;
    
    static Cluster z1z3Shuffle;
    static Cluster z1z3z5Shuffle;
    
    static Cluster z1z3ClusterExpansionNN;
    static Cluster z1z3z5ClusterExpansionNNN;
    static Cluster z1z3ClusterExpansionPP;
    static Cluster z1z3z5ClusterExpansionPPP;
    
    static List<StoreDefinition> z1z3Stores;
    static List<StoreDefinition> z1z3z5Stores;

    @BeforeClass
    public static void setup() {
        zzCurrent = ClusterTestUtils.getZZCluster();
        zzShuffle = ClusterTestUtils.getZZClusterWithSwappedPartitions();
        zzClusterExpansionNN = ClusterTestUtils.getZZClusterWithNN();
        zzClusterExpansionPP = ClusterTestUtils.getZZClusterWithPP();
        zzStores = ClusterTestUtils.getZZStoreDefsBDB();

        zzzCurrent = ClusterTestUtils.getZZZCluster();

        zzzShuffle = ClusterTestUtils.getZZZClusterWithSwappedPartitions();
        zzzClusterExpansionNNN = ClusterTestUtils.getZZZClusterWithNNN();
        zzzClusterExpansionPPP = ClusterTestUtils.getZZZClusterWithPPP();
        zzeZoneExpansion = ClusterTestUtils.getZZECluster();
        zzzZoneExpansionXXP = ClusterTestUtils.getZZEClusterXXP();
        zzzStores = ClusterTestUtils.getZZZStoreDefsBDB();
    }
    
    @BeforeClass
    public static void setupNonContigous() {
        z1z3Current = ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIds();
        z1z3z5Current = ClusterTestUtils.getZ1Z3Z5ClusterWithNonContiguousNodeIds();
        z1z3ClusterExpansionNN = ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIdsWithNN();
        z1z3ClusterExpansionPP = ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIdsWithPP();
        
        z1z3z5ClusterExpansionNNN = ClusterTestUtils.getZ1Z3Z5ClusterWithNonContiguousNodeIdsWithNNN();
        z1z3z5ClusterExpansionPPP = ClusterTestUtils.getZ1Z3Z5ClusterWithNonContiguousNodeIdsWithPPP();
        
        z1z3Shuffle = ClusterTestUtils.getZ1Z3ClusterWithNonContiguousNodeIdsWithSwappedPartitions();
        z1z3z5Shuffle = ClusterTestUtils.getZ1Z3Z5ClusterWithNonContiguousNodeIdsWithSwappedPartitions();
        
        z1z3Stores = ClusterTestUtils.getZ1Z3StoreDefsBDB();
        z1z3z5Stores = ClusterTestUtils.getZ1Z3Z5StoreDefsBDB();   
    }

    @Test
    public void testNoop() {
        List<RebalanceTaskInfo> batchPlan;

        // Two zones
        batchPlan = ClusterTestUtils.getBatchPlan(zzCurrent, zzCurrent, zzStores);
        assertEquals(batchPlan.size(), 0);

        // Three zones
        batchPlan = ClusterTestUtils.getBatchPlan(zzzCurrent, zzzCurrent, zzzStores);
        assertEquals(batchPlan.size(), 0);
    }

    @Test
    public void testShuffle() {
        List<RebalanceTaskInfo> batchPlan;

        // Two zones
        batchPlan = ClusterTestUtils.getBatchPlan(zzCurrent, zzShuffle, zzStores);
        assertTrue(batchPlan.size() > 0);

        // Three zones
        batchPlan = ClusterTestUtils.getBatchPlan(zzzCurrent, zzzShuffle, zzzStores);
        assertTrue(batchPlan.size() > 0);
    }

    @Test
    public void testClusterExpansion() {
        List<RebalanceTaskInfo> batchPlan;

        // Two zones
        batchPlan = ClusterTestUtils.getBatchPlan(zzClusterExpansionNN,
                                                  zzClusterExpansionPP,
                                                  zzStores);
        assertTrue(batchPlan.size() > 0);

        // Three zones
        batchPlan = ClusterTestUtils.getBatchPlan(zzzClusterExpansionNNN,
                                                  zzzClusterExpansionPPP,
                                                  zzzStores);
        assertTrue(batchPlan.size() > 0);
    }

    @Test
    public void testZoneExpansion() {
        List<RebalanceTaskInfo> batchPlan;

        // Two-to-three zones (i)
        batchPlan = ClusterTestUtils.getBatchPlan(zzeZoneExpansion, zzzZoneExpansionXXP, zzzStores);
        assertTrue(batchPlan.size() > 0);

        // Two-to-three zones (ii)
        batchPlan = ClusterTestUtils.getBatchPlan(zzCurrent,
                                                  zzStores,
                                                  zzzZoneExpansionXXP,
                                                  zzzStores);
        assertTrue(batchPlan.size() > 0);

        // Two-to-three zones (iii)
        batchPlan = ClusterTestUtils.getBatchPlan(zzCurrent, zzStores, zzzCurrent, zzzStores);
        assertTrue(batchPlan.size() > 0);
    }
    
    @Test
    public void testNoopWithNonContiguousZoneIdsAndNodeIds() {
        List<RebalanceTaskInfo> batchPlan;

        // Two zones
        batchPlan = ClusterTestUtils.getBatchPlan(z1z3Current, z1z3Current, z1z3Stores);
        assertEquals(batchPlan.size(), 0);

        // Three zones
        batchPlan = ClusterTestUtils.getBatchPlan(z1z3z5Current, z1z3z5Current, z1z3z5Stores);
        assertEquals(batchPlan.size(), 0);
    }
    
    @Test
    public void testShuffleWithNonContiguousZoneIdsAndNodeIds() {
        List<RebalanceTaskInfo> batchPlan;

        // Two zones
        batchPlan = ClusterTestUtils.getBatchPlan(z1z3Current, z1z3Shuffle, z1z3Stores);
        assertTrue(batchPlan.size() > 0);

        // Three zones
        batchPlan = ClusterTestUtils.getBatchPlan(z1z3z5Current, z1z3z5Shuffle, z1z3z5Stores);
        assertTrue(batchPlan.size() > 0);
    }

    @Test
    public void testClusterExpansionWithNonContiguousZoneIdsAndNodeIds() {
        List<RebalanceTaskInfo> batchPlan;
        // Two zones
        batchPlan = ClusterTestUtils.getBatchPlan(z1z3ClusterExpansionNN,
                                                  z1z3ClusterExpansionPP,
                                                  z1z3Stores);
        assertTrue(batchPlan.size() > 0);
        // Three zones
        batchPlan = ClusterTestUtils.getBatchPlan(z1z3z5ClusterExpansionNNN,
                                                  z1z3z5ClusterExpansionPPP,
                                                  z1z3z5Stores);
        assertTrue(batchPlan.size() > 0);
    }


}
