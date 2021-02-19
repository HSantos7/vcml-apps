package voldemort.consistency.types;

import voldemort.consistency.types.Message.Type;

public class Content<K, V> {
    private K key;
    private V value;

    public Content(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public Content(K key) {
        this.key = key;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }


    public void setValue(V value) {
        this.value = value;
    }
}

