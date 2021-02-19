package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.types.Message;
import edu.msu.cse.cops.server.consistency.types.MetaData;

public interface QuorumInterface<K,V> {
    void waitQuorum(Message<K, V> message);
    boolean isQuorumSatisfied(Message<K, V> message);
    boolean isZonesSatisfied(Message<K, V> message);
}
