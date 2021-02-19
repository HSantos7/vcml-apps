package voldemort.consistency;

import voldemort.consistency.types.Message;

public class Quorum<K, V> implements QuorumInterface<K, V> {
    public Quorum() {
    }

    @Override
    public void waitQuorum(Message<K, V> message) {

    }

    @Override
    public boolean isQuorumSatisfied(Message<K, V> message) {
        return message.getMetaData().getPutSuccesses() >= Constants.requiredWrites;
    }

    @Override
    public boolean isZonesSatisfied(Message<K, V> message) {
        boolean zonesSatisfied = false;
        if(Constants.requiredZones == 0) {
            zonesSatisfied = true;
        } else {
            int numZonesSatisfied = message.getMetaData().getZoneResponses().size();
            if(numZonesSatisfied >= (Constants.requiredZones + 1)) {
                zonesSatisfied = true;
            }
        }
        return zonesSatisfied;
    }
}
