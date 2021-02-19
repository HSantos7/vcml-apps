package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.MainClass;
import edu.msu.cse.cops.server.consistency.cluster.Node;
import edu.msu.cse.cops.server.consistency.utils.ByteArray;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class GroupMembership implements GroupMembershipInterface{
    private Node mySelf;

    public GroupMembership(Node node) {
        this.mySelf = node;
    }

    @Override
    public ArrayList<Node> getReplicationTargets(boolean async) {
        ArrayList<Node> replicationNodes = new ArrayList<>();
        for (int i = 0; i < MainClass.gServer.getNumOfDatacenters(); i++) {
            if (i == getDcId())
                continue;
            String id = i + "_" + getPartId();
            replicationNodes.add(new Node(id,"",-1,-1,-1,new ArrayList<>(1)));
        }
        return  replicationNodes;
    }

    public List<Node> getNodes(ByteArray key, int required) {
        return null;
    }

    @Override
    public Boolean getRole() {
        return true;
    }

    @Override
    public int getTimestamper() {
        return -1;
    }

    @Override
    public int findPartition(String key) {
        long hash = 0;
        try {
            hash = edu.msu.cse.dkvf.Utils.getMd5HashLong(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return (int) (hash % MainClass.gServer.getNumOfPartitions());
    }
    @Override
    public int getDcId() {
        return this.mySelf.getZoneId();
    }
    @Override
    public int getPartId() {
        return this.mySelf.getPartitionIds().get(0);
    }
    @Override
    public String getNodeId() {
        return String.valueOf(getDcId()) + String.valueOf(getPartId());
    }
    @Override
    public Node getMySelf() {return mySelf;}
}
