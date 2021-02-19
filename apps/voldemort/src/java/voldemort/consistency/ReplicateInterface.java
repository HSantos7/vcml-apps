package voldemort.consistency;

import voldemort.consistency.types.Content;
import voldemort.consistency.types.Message;
import voldemort.consistency.types.MetaData;

public interface ReplicateInterface<K,V> {
    void replicate(Message<K, V> message);
    boolean apply(Message<K, V> message);
}
