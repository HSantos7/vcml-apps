package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.types.Message;

public interface API<K,V>{
    void newMessage(Message<K, V> incomingMessage);
    void replicateMessage(Message<K, V> replicateMessage);
    void getReplicateState();
}
