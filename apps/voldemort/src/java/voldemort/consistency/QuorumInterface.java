package voldemort.consistency;

import voldemort.consistency.types.Message;

public interface QuorumInterface<K, V> {
    void waitQuorum(Message<K, V> message);
    boolean isQuorumSatisfied(Message<K, V> message);
    boolean isZonesSatisfied(Message<K, V> message);
}
