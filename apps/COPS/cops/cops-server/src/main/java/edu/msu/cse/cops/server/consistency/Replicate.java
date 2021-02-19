package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.cluster.Node;
import edu.msu.cse.cops.server.consistency.types.Content;
import edu.msu.cse.cops.server.consistency.types.Message;
import edu.msu.cse.cops.server.consistency.types.MetaData;

import java.util.List;


public class Replicate<K,V> implements ReplicateInterface<K,V> {
    GroupMembershipInterface groupMembership;
    QuorumInterface<K,V> quorum;
    CommunicationInterface.external<K,V> communicationRemote;
    DeliveryConditionInterface<K,V> deliveryCondition;

    public Replicate(GroupMembershipInterface groupMembership, QuorumInterface<K,V> quorum, CommunicationInterface.internal<K,V> communicationLocal, CommunicationInterface.external<K,V> communicationRemote, DeliveryConditionInterface<K,V> deliveryCondition) {
        this.groupMembership = groupMembership;
        this.quorum = quorum;
        this.communicationRemote = communicationRemote;
        this.deliveryCondition = deliveryCondition;
    }

    @Override
    public void replicate(Message<K, V> message) {
        List<Node> targets = groupMembership.getReplicationTargets(false);
        for (Node target: targets) {
            communicationRemote.replicate(target, message.getContent(), message.getMetaData(), null);
        }
        quorum.waitQuorum(message);
    }

    @Override
    public boolean apply(Message<K, V> message) {
        return deliveryCondition.tryToApply(message);
    }
}
