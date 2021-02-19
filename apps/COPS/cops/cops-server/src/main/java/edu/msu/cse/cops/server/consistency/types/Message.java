package edu.msu.cse.cops.server.consistency.types;

public class Message<K,V> {
    public enum Type {
        PUT,
        GET,
        GET_VERSIONS,
        DELETE,
        REPLICATE,
        DEPENDENCY_REQUEST,
        DEPENDENCY_RESPONSE,
    }

    Type type;
    Content<K,V> content;
    MetaData metaData;

    public Message(Type type, Content<K,V> content, MetaData metaData) {
        this.type = type;
        this.content = content;
        this.metaData = metaData;
    }

    public Content<K, V> getContent() {
        return content;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    public Type getType() {
        return type;
    }
}
