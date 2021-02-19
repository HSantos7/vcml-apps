package voldemort.consistency;

import voldemort.consistency.cluster.Node;
import voldemort.consistency.utils.ByteArray;
import voldemort.store.Store;

import java.util.List;
import java.util.Map;

public interface GroupMembershipInterface {
    Map<Integer, ?> getReplicationTargets(boolean async);

    List<Node> getNodes(ByteArray key, int required);

    Boolean getRole(int node);
    int getTimestamper();

    int getDcId();

    int getPartId();

    int findPartition(String key);

    Node getMySelf();
}
