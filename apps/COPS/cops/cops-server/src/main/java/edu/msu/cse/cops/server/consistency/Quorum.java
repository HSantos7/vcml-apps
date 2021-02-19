package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.types.Message;

public class Quorum<K,V> implements QuorumInterface<K,V> {
    public Quorum() {
    }

    @Override
    public void waitQuorum(Message<K, V> message) {
        for (;;) {
            if (isQuorumSatisfied(message) && isZonesSatisfied(message)) {
                return;
            }
            try {
                //We should avoid busy waiting. Could be fixed with wait/notify
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public boolean isQuorumSatisfied(Message<K, V> message) {
        return true;
    }

    @Override
    public boolean isZonesSatisfied(Message<K, V> message) {
        return true;
    }
}
