package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.types.DependencyRequest;
import edu.msu.cse.cops.server.consistency.types.Message;

public interface DeliveryConditionInterface<K,V> {
    boolean tryToApply(Message<K,V> message);

    void addToRemoteWaitingDep(DependencyRequest<K> kDependencyRequest);

    boolean removeRemoteWaitingDep(Message<K, V> incomingMessage);
}
