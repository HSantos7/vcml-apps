package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.types.Content;
import edu.msu.cse.cops.server.consistency.types.Message;
import edu.msu.cse.cops.server.consistency.types.MetaData;

public interface ReplicateInterface<K,V> {
    void replicate(Message<K, V> message);
    boolean apply(Message<K, V> message);
}
