package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.cluster.Node;
import edu.msu.cse.cops.server.consistency.types.Content;
import edu.msu.cse.cops.server.consistency.types.Message;
import edu.msu.cse.cops.server.consistency.types.MetaData;
import edu.msu.cse.cops.server.consistency.versioning.Version;

public interface CommunicationInterface {
    interface internal<K,V>{
        void get(Node node, Content<K,V> content, MetaData metaData, Callback callback);
        void put(Node node, Content<K,V> content, MetaData metaData);
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
