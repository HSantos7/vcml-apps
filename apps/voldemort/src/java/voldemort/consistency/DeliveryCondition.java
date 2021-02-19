package voldemort.consistency;

import voldemort.consistency.types.DependencyRequest;
import voldemort.consistency.types.Message;

public class DeliveryCondition<K,V> implements DeliveryConditionInterface<K,V> {
    public DeliveryCondition(OrderInterface<K,V> order, CommunicationInterface.internal<K,V> communicationLocal, CommunicationInterface.external<K,V> communicationRemote, GroupMembershipInterface groupMembership) {
    }

    @Override
    public boolean tryToApply(Message<K, V> message) {
        return false;
    }

    @Override
    public void addToWaitingDepChecks(DependencyRequest<K> kDependencyRequest) {

    }

    @Override
    public boolean removeKeyWaitingDependency(Message<K, V> incomingMessage) {
        return false;
    }
}
