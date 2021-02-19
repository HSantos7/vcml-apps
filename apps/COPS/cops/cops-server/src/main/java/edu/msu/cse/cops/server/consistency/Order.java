package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.types.Content;
import edu.msu.cse.cops.server.consistency.utils.BitsUtils;
import edu.msu.cse.cops.server.consistency.utils.SystemTime;
import edu.msu.cse.cops.server.consistency.utils.Time;
import edu.msu.cse.cops.server.consistency.utils.Utils;
import edu.msu.cse.cops.server.consistency.versioning.Occurred;
import edu.msu.cse.cops.server.consistency.versioning.Version;

import java.util.Map;

public class Order<K,V> implements OrderInterface<K,V>{
    GroupMembershipInterface groupMembership;
    protected final Time time;
    protected int metadataRefreshAttempts;
    CommunicationInterface.internal<K,V> communicationLocal;

    Version clock;

    public Order(int maxMetadataRefreshAttempts, GroupMembershipInterface groupMembership,  CommunicationInterface.internal<K,V> communicationLocal) {
        this.time = Utils.notNull(SystemTime.INSTANCE);
        this.metadataRefreshAttempts = maxMetadataRefreshAttempts;
        this.groupMembership = groupMembership;
        this.communicationLocal = communicationLocal;
        this.clock = Configurations.getVersionObject();
    }

    @Override
    public Version timeStamping(Content<K, V> content, long startTime) {
        updateClock();
        long lowerBits = BitsUtils.getLowerBits(groupMembership.getDcId());
        Version toReturn = Configurations.getVersionObject();
        if (toReturn != null) {
            //toReturn.updateVersion(groupMembership.getNodeId(),getClock().getVersions().get(groupMembership.getNodeId()) + lowerBits, System.currentTimeMillis());
            if (getClock().getVersions().size() == 1)
                toReturn.updateVersion(groupMembership.getNodeId(),getClock().getVersions().entrySet().iterator().next().getValue() + lowerBits, System.currentTimeMillis());
            else
                toReturn.updateVersion(groupMembership.getNodeId(),getClock().getVersions().get(groupMembership.getNodeId()) + lowerBits, System.currentTimeMillis());
        }
        return toReturn;
    }

    @Override
    public Occurred compareMessages(Version ver1, Version ver2) {
        return ver1.compare(ver2);
    }

    @Override
    public void updateClock() {
        synchronized (getClock()) {
            if (getClock().getVersions().size() == 1)
                getClock().updateVersion(groupMembership.getNodeId(),getClock().getVersions().entrySet().iterator().next().getValue() + BitsUtils.shiftToHighBits(1), System.currentTimeMillis());
            else
                getClock().updateVersion(groupMembership.getNodeId(),getClock().getVersions().getOrDefault(groupMembership.getNodeId(), 0L) + BitsUtils.shiftToHighBits(1), System.currentTimeMillis());
        }
    }

    @Override
    public void updateClock(Map<String, Long> newVersion) {
        for (Map.Entry<String, Long> entry : newVersion.entrySet()){
            //Lamport logical clock algorithm
            long newVersionInHigherBits = BitsUtils.getHigherBits(entry.getValue());

            long newHigherBits = Math.max(entry.getValue(), newVersionInHigherBits) + BitsUtils.shiftToHighBits(1);
            getClock().updateVersion(entry.getKey(), newHigherBits, System.currentTimeMillis());
        }
    }

    public Version getClock() {
        return this.clock;
    }

    public void setClock(Version clock) {
        this.clock = clock;
    }

}
