package voldemort.consistency;

import voldemort.consistency.types.DependencyRequest;
import voldemort.consistency.types.Message;

public interface DeliveryConditionInterface<K,V> {
    boolean tryToApply(Message<K,V> message);

    void addToWaitingDepChecks(DependencyRequest<K> kDependencyRequest);

    boolean removeKeyWaitingDependency(Message<K, V> incomingMessage);
}
