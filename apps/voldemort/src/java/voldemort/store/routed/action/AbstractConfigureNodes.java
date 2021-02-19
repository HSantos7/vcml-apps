/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.routed.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.consistency.cluster.Node;
import voldemort.consistency.cluster.Zone;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.routing.RoutingStrategy;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.routed.Pipeline.Event;
import voldemort.store.routed.PipelineData;
import voldemort.store.routed.PutPipelineData;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.utils.ByteUtils;

public abstract class AbstractConfigureNodes<K, V, PD extends PipelineData<K, V>> extends
        AbstractAction<K, V, PD> {

    protected final FailureDetector failureDetector;

    protected final int required;

    protected final RoutingStrategy routingStrategy;

    protected AbstractConfigureNodes(PD pipelineData,
                                     Event completeEvent,
                                     FailureDetector failureDetector,
                                     int required,
                                     RoutingStrategy routingStrategy) {
        super(pipelineData, completeEvent);
        this.failureDetector = failureDetector;
        this.required = required;
        this.routingStrategy = routingStrategy;
    }

    protected List<Node> getNodes(ByteArray key) {
        List<Node> nodes = new ArrayList<Node>();

        pipelineData.setReplicationSet(routingStrategy.routeRequest(key.get()));
        // raise an error if no server has any partitions defined
        if(pipelineData.getReplicationSet().size() == 0) {
            throw new IllegalArgumentException("All servers configured with no partitions");
        }

        for(Node node: pipelineData.getReplicationSet()) {
            if(failureDetector.isAvailable(node))
                nodes.add(node);
            else {
                if(pipelineData instanceof PutPipelineData) {
                    ((PutPipelineData) pipelineData).getSynchronizer().tryDelegateSlop(node);
                }
                pipelineData.addFailedNode(node);
                if(logger.isDebugEnabled()) {
                    logger.debug("Key " + ByteUtils.toHexString(key.get()) + " Node "
                                 + node.getId() + " down");
                }
            }
        }

        if(nodes.size() < required) {
            List<Integer> failedNodes = new ArrayList<Integer>();
            List<Integer> allNodes = new ArrayList<Integer>();
            for(Node node: pipelineData.getReplicationSet()) {
                allNodes.add(node.getId());
            }
            for(Node node: pipelineData.getFailedNodes()) {
                failedNodes.add(node.getId());
            }
            String errorMessage = "Only " + nodes.size() + " nodes up in preference list"
                                  + ", but " + required + " required. Replication set: " + allNodes
                                  + "Nodes down: " + failedNodes;
            if(logger.isDebugEnabled()) {
                logger.debug(errorMessage);
            }
            throw new InsufficientOperationalNodesException(errorMessage);
        }
        return nodes;
    }

    public static Map<Integer, List<Node>> convertToZoneNodeMap(List<Node> nodes) {
        // Create zone id to node mapping
        Map<Integer, List<Node>> zoneIdToNode = new HashMap<Integer, List<Node>>();
        for(Node node: nodes) {
            List<Node> nodesList = null;
            if(zoneIdToNode.containsKey(node.getZoneId())) {
                nodesList = zoneIdToNode.get(node.getZoneId());
            } else {
                nodesList = new ArrayList<Node>();
                zoneIdToNode.put(node.getZoneId(), nodesList);
            }
            nodesList.add(node);
        }
        return zoneIdToNode;
    }

    public void validateZonesRequired(Zone clientZone, int zonesRequired) {
        if(zonesRequired > clientZone.getProximityList().size()) {
            throw new VoldemortException("Number of zones required (" + zonesRequired
                                         + ") for zone id (" + clientZone.getId()
                                         + ") should be less than the total number of zones "
                                         + +clientZone.getProximityList().size());
        }

        if(zonesRequired > required) {
            throw new VoldemortException("Number of zones required ("
                                         + zonesRequired
                                         + ") for zone id ("
                                         + clientZone.getId()
                                         + ") should be less than the required number of zones for operation "
                                         + required);
        }

    }
}
