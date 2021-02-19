package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.cluster.Node;

import java.util.ArrayList;

public interface GroupMembershipInterface {
    ArrayList<Node> getReplicationTargets(boolean async);
    Boolean getRole();
    int getTimestamper();

    String getNodeId();

    int getDcId();

    int getPartId();

    int findPartition(String key);

    Node getMySelf();
}
