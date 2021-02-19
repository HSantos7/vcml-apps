package voldemort.consistency;

import voldemort.consistency.cluster.Node;
import voldemort.consistency.types.Content;
import voldemort.consistency.types.Message;
import voldemort.consistency.types.MetaData;

public class CommunicationRemote<K,V> implements CommunicationInterface.external<K,V> {
    GroupMembershipInterface groupMembership;
    public CommunicationRemote(GroupMembershipInterface groupMembership) {
        this.groupMembership = groupMembership;
    }

    //public interface
    @Override
    public void sendGetResponse(Content<K, V> content, MetaData metaData, Callback callback) {
    }

    @Override
    public void sendPutResponse(Content<K, V> content, MetaData metaData, Callback callback) {
    }

    @Override
    public void sendDeleteResponse(Content<K, V> content, MetaData metaData, Callback callback) {

    }

    @Override
    public void replicate(Node node, Content<K, V> content, MetaData metaData, Callback callback) {
    }

    @Override
    public void sendDependenciesCheck(Content<K, V> content, MetaData metaData){

    }

    @Override
    public void sendDependenciesResponse(Message<K, V> message) {

    }
}
