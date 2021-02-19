package voldemort.consistency;

import voldemort.VoldemortClientShell;
import voldemort.consistency.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.utils.ByteUtils;
import voldemort.store.InsufficientOperationalNodesException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GroupMembership implements GroupMembershipInterface{

    public GroupMembership(Node node) {

    }

    @Override
    public Map<Integer,?> getReplicationTargets(boolean async) {
        if (async)
            return VoldemortClientShell.factory.getNonblockingStores();
        return VoldemortClientShell.factory.getStores();
    }

    @Override
    public List<Node> getNodes(ByteArray key, int required) {
        List<Node> nodes = new ArrayList<Node>();
        List<Node> failedReplicationSet = new ArrayList<Node>();

        List<Node> replicationSet = VoldemortClientShell.routingStrategy.routeRequest(key.get());
        FailureDetector failureDetector = VoldemortClientShell.factory.getFailureDetector();
        for(Node node: replicationSet) {
            if(failureDetector.isAvailable(node))
                nodes.add(node);
            else {
                failedReplicationSet.add(node);
                System.out.println("Key " + ByteUtils.toHexString(key.get()) + " Node "
                            + node.getId() + " down");
                }
            }

        if(nodes.size() < required) {
            List<Integer> failedNodes = new ArrayList<Integer>();
            List<Integer> allNodes = new ArrayList<Integer>();
            for(Node node: replicationSet) {
                allNodes.add(node.getId());
            }
            for(Node node: failedReplicationSet) {
                failedNodes.add(node.getId());
            }
            String errorMessage = "Only " + nodes.size() + " nodes up in preference list"
                    + ", but " + required + " required. Replication set: " + allNodes
                    + "Nodes down: " + failedNodes;
            System.out.println(errorMessage);
            throw new InsufficientOperationalNodesException(errorMessage);
        }
        return nodes;
    }

    @Override
    public Boolean getRole(int node) {
        return Constants.permissions.get(node);
    }

    @Override
    public int getTimestamper() {
        for (int node = 0; node < Constants.permissions.size(); node++){
            if (Constants.permissions.get(node))
                return node;
        }
        return -1;
    }

    @Override
    public Node getMySelf() {
        return null;
    }

    @Override
    public int getDcId() {
        return -1;
    }

    @Override
    public int getPartId() {
        return -1;
    }

    @Override
    public int findPartition(String key) {
        return 0;
    }
}
