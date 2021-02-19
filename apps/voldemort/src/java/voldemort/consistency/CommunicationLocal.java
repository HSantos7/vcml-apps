package voldemort.consistency;

import voldemort.VoldemortClientShell;
import voldemort.client.DefaultStoreClient;
import voldemort.consistency.cluster.Node;
import voldemort.consistency.types.Content;
import voldemort.consistency.types.MetaData;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.versioning.Version;
import voldemort.consistency.versioning.Versioned;
import voldemort.store.Store;
import voldemort.store.nonblockingstore.NonblockingStore;

import java.util.List;
import java.util.Map;


public class CommunicationLocal<K,V> implements CommunicationInterface.internal<K,V> {
    GroupMembershipInterface groupMembership;
    DefaultStoreClient<K,V> defaultStore;
    public CommunicationLocal(GroupMembershipInterface groupMembership) {
        this.groupMembership = groupMembership;
        this.defaultStore = (DefaultStoreClient<K, V>) VoldemortClientShell.client;

    }
    //private interface
    @Override
    public void get(Node node, Content<K, V> content, MetaData metaData, Callback callback) {
        NonblockingStore store = (NonblockingStore) groupMembership.getReplicationTargets(true).get(node.getId());
        store.submitGetRequest(new ByteArray(VoldemortClientShell.serializeKey(content.getKey())), null, callback, Constants.getOpTimeoutInMs);
        /*else if (content.getType() == Message.Type.GET_VERSIONS)
            store.submitGetVersionsRequest(new ByteArray(VoldemortClientShell.serializeKey(content.getKey())), callback, Constants.getOpTimeoutInMs);
       */
    }

    @Override
    public void delete(Node node, Content<K, V> content, MetaData metaData, Callback callback) {
        NonblockingStore store = (NonblockingStore) groupMembership.getReplicationTargets(true).get(node.getId());
        store.submitDeleteRequest(new ByteArray(VoldemortClientShell.serializeKey(content.getKey())), metaData.getVersion(), callback, Constants.deleteOpTimeoutInMs);
    }


    @Override
    public void put(Node node, Content<K, V> content, MetaData metaData, Callback callback) {
        if (callback != null){
            NonblockingStore store = (NonblockingStore) groupMembership.getReplicationTargets(true).get(node.getId());
            store.submitPutRequest(new ByteArray(VoldemortClientShell.serializeKey(content.getKey())), (Versioned<byte[]>) metaData.getVersioned(), null, callback, Constants.getOpTimeoutInMs);
            return;
        }
        Map<Integer, Store<ByteArray, byte[], byte[]>> stores = (Map<Integer, Store<ByteArray, byte[], byte[]>>) groupMembership.getReplicationTargets(false);
        stores.get(node.getId()).put(new ByteArray(VoldemortClientShell.serializeKey(content.getKey())), (Versioned<byte[]>) metaData.getVersioned(), null);
    }

    @Override
    public Version getActualVersion(Content<K, V> content, MetaData metaData) {
        return defaultStore.getVersionForPut(content.getKey());
    }

    public List<Node> getNodes(ByteArray key, int required){
        return null;
    }
}
