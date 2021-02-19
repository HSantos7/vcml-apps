package voldemort.consistency;

import voldemort.consistency.cluster.Node;
import voldemort.consistency.types.Content;
import voldemort.consistency.types.Message;
import voldemort.consistency.types.MetaData;
import voldemort.consistency.versioning.Version;

public interface CommunicationInterface<K,V> {
    interface internal<K,V>{
        void get(Node node, Content<K,V> content, MetaData metaData, Callback callback);
        void put(Node node, Content<K,V> content, MetaData metaData, Callback callback);
        void delete(Node node, Content<K,V> content, MetaData metaData, Callback callback);

        Version getActualVersion(Content<K, V> content, MetaData metaData);

    }
    interface external<K,V> {
        void sendGetResponse(Content<K, V> content, MetaData metaData, Callback callback);
        void sendPutResponse(Content<K, V> content, MetaData metaData, Callback callback);
        void sendDeleteResponse(Content<K, V> content, MetaData metaData, Callback callback);
        void replicate(Node node, Content<K, V> content, MetaData metaData, Callback callback);
        void sendDependenciesCheck(Content<K, V> content, MetaData metaData);
        void sendDependenciesResponse(Message<K, V> message);
    }
}
