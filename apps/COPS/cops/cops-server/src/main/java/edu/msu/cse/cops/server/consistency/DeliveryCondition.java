package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.types.*;
import edu.msu.cse.cops.server.consistency.versioning.Occurred;
import edu.msu.cse.cops.server.consistency.versioning.Version;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeliveryCondition<K,V> implements DeliveryConditionInterface<K,V> {
    CommunicationInterface.internal<K,V> communicationLocal;
    CommunicationInterface.external<K,V> communicationRemote;
    OrderInterface<K,V> order;
    GroupMembershipInterface groupMembership;

    HashMap<K, List<K>> waitingLocalDeps; //the key is the key that we want as dependency, the value is the list of pending keys that are waiting. Both dependency key and pending key are hosted on this partition, that why it is called local dep check.
    // dependency check mechanism
    HashMap<K, List<DependencyRequest<K>>> waitingDepChecks; //the key is the key that we want as dependency, and value is the dependency that is waiting for this key.
    final HashMap<K, PendingMessage<K,V>> pendingKeys; //the key is the key that is pending, and the value is the record to write + its dependencies.

    public DeliveryCondition(OrderInterface<K,V> order, CommunicationInterface.internal<K,V> communicationLocal, CommunicationInterface.external<K,V> communicationRemote, GroupMembershipInterface groupMembership) {
        this.communicationLocal = communicationLocal;
        this.communicationRemote = communicationRemote;
        this.order = order;
        this.groupMembership = groupMembership;
        this.waitingLocalDeps = new HashMap<>();
        this.waitingDepChecks = new HashMap<>();
        this.pendingKeys = new HashMap<>();

    }

    @Override
    public boolean tryToApply(Message<K,V> message) {
        Content<K,V> content = message.getContent();
        MetaData metaData = message.getMetaData();
        switch (message.getType()){
            case PUT:{
                return makeVisible(content, metaData);
            }
            case REPLICATE:{
                order.updateClock(metaData.getVersion().getVersions());
                if (localDepChecking(content, metaData))
                    return makeVisible(content, metaData);
                else{
                    putPendingKeys(content, metaData);
                    communicationRemote.sendDependenciesCheck(content,metaData);
                    return false;
                }
            }
            case DEPENDENCY_RESPONSE:
                return makeVisible(content, metaData);
        }
        return false;
    }

    private boolean makeVisible(Content<K, V> content, MetaData metaData){
        // first we check the current version, maybe it is higher than the
        // version that we want to write. In that case we don't write it.
        Version version = communicationLocal.getActualVersion(content, metaData);
        MetaData currentVer = new MetaData();
        currentVer.setVersion(version);
        if (version != null) {
            //if (currentRec.getVersion() >= metaData.getVersion())
            Occurred compare = order.compareMessages(currentVer.getVersion(), metaData.getVersion());
            if (compare == Occurred.BEFORE || compare == Occurred.TIE){

                return true;
            }
        }
        // If current version is older, we go ahead, and write the given
        // version.
        communicationLocal.put(null, content, metaData);
        if (metaData.getPutSuccesses() >= Configurations.requiredWrites) {
            postVisibility(content, metaData);
            return true;
        } else{
            return false;
        }
    }

    private void postVisibility(Content<K, V> content, MetaData metaData) {
        //Two types of partitions may wait for visibility of version: 1) local partition, or 2) another partition.
        //We check both cases.

        //local dep check
        List<Message<K, V>> applyWaiting = tryToSolveWaitingDependency(content, metaData);
        for (Message<K, V> msgToApply : applyWaiting) {
            tryToApply(msgToApply);
        }

        //check other requests from other partitions
        List<Message<K, V>> toAnswerMessage = tryToSolveRemoteWaitingDependency(content, metaData);
        for (Message<K, V> msgToAnswer : toAnswerMessage) {
            communicationRemote.sendDependenciesResponse(msgToAnswer);
        }
    }

    private void addToWaitingLocalDeps(K wantedKey, K pendingKey) {
        if (waitingLocalDeps.containsKey(wantedKey)) {
            if (!waitingLocalDeps.get(wantedKey).contains(pendingKey)) {
                waitingLocalDeps.get(wantedKey).add(pendingKey);
            }
        } else {
            List<K> newPendingKeyList = new ArrayList<>();
            newPendingKeyList.add(pendingKey);
            waitingLocalDeps.put(wantedKey, newPendingKeyList);
        }
    }

    public List<Message<K, V>> tryToSolveWaitingDependency(Content<K, V> content, MetaData metaData) {
        List<Message<K, V>> toApply = new ArrayList<>();
        try {
            synchronized (pendingKeys) {
                if (waitingLocalDeps.containsKey(content.getKey())) {
                    for (K pendingKey : waitingLocalDeps.get(content.getKey())) {
                        if (pendingKeys.containsKey(pendingKey)) {
                            List<Dependency<K>> oldPending = new ArrayList<>(pendingKeys.get(pendingKey).getDependencies());
                            for (Dependency<K> dep : oldPending) {
                                //if (dep.getKey().equals(content.getKey()) && dep.getVersion().getTimestamp() <= metaData.getVersion().getTimestamp()) {
                                if (dep.getKey().equals(content.getKey())) {
                                    Occurred compare = order.compareMessages(dep.getVersion(),metaData.getVersion());
                                    if (compare == Occurred.AFTER || compare == Occurred.TIE) {
                                        pendingKeys.get(pendingKey).getDependencies().remove(dep);
                                        if (pendingKeys.get(pendingKey).getDependencies().isEmpty()) {
                                            //makeVisible(pendingKey, pendingKeys.get(pendingKey).record);
                                            MetaData md = new MetaData();
                                            Version version = Configurations.getVersionObject();
                                            //version.updateVersion(groupMembership.getNodeId(),pendingKeys.get(pendingKey).getVersion().getTimestamp(),System.currentTimeMillis());
                                            for (Map.Entry<String, Long> entry : pendingKeys.get(pendingKey).getVersion().getVersions().entrySet()) {
                                                version.updateVersion(entry.getKey(), entry.getValue(), System.currentTimeMillis());
                                            }

                                            md.setVersion(version);
                                            toApply.add(new Message<>(Message.Type.PUT, new Content<>(pendingKey, pendingKeys.get(pendingKey).getValue()), md));
                                            //pendingKeys.remove(pendingKey);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Error in PostVisibility: " + e);
        }
        return toApply;
    }

    @Override
    public void addToRemoteWaitingDep(DependencyRequest<K> dependencyRequest) {
        if (waitingDepChecks.containsKey(dependencyRequest.getDependency().getKey())) {
            waitingDepChecks.get(dependencyRequest.getDependency().getKey()).add(dependencyRequest);
        } else {
            List<DependencyRequest<K>> drs = new ArrayList<>();
            drs.add(dependencyRequest);
            waitingDepChecks.put(dependencyRequest.getDependency().getKey(), drs);
        }
    }

    @Override
    public boolean removeRemoteWaitingDep(Message<K, V> incomingMessage) {
        int metIndex = -1;
        if (pendingKeys.containsKey(incomingMessage.getContent().getKey())) {
            for (int i = 0; i < pendingKeys.get(incomingMessage.getContent().getKey()).getDependencies().size(); i++) {
                Dependency<K> dep = pendingKeys.get(incomingMessage.getContent().getKey()).getDependencies().get(i);
                Map.Entry<String,Version> dependency = incomingMessage.getMetaData().getDependencies().entrySet().iterator().next();
                if (dep.getKey().equals(dependency.getKey())) {
                    //if (dep.getVersion().getTimestamp() <= dependency.getValue().getTimestamp()) {
                    Occurred compare = order.compareMessages(dep.getVersion(),dependency.getValue());
                    if (compare == Occurred.AFTER || compare == Occurred.TIE) {
                        metIndex = i;
                        break;
                    }
                }
            }
        }
        if (metIndex >= 0) {
            pendingKeys.get(incomingMessage.getContent().getKey()).getDependencies().remove(metIndex);
            if (pendingKeys.get(incomingMessage.getContent().getKey()).getDependencies().isEmpty()) {
                Version version = Configurations.getVersionObject();
                for (Map.Entry<String, Long> entry : pendingKeys.get(incomingMessage.getContent().getKey()).getVersion().getVersions().entrySet()){
                    version.updateVersion(entry.getKey(), entry.getValue(), System.currentTimeMillis());
                }
                //TODO TALVEZ NAO SEJA ISTO!
                //incomingMessage.getContent().setValue((V) version.getTimestamp());
                //makeVisible(drm.getForKey(), pendingKeys.get(incomingMessage.getContent().getKey()).record);
                return true;
            }
        }
        return false;
    }

    public List<Message<K, V>> tryToSolveRemoteWaitingDependency(Content<K, V> content, MetaData metaData) {
        List<Message<K, V>> toAnswer = new ArrayList<>();
        if (waitingDepChecks.containsKey(content.getKey())) {
            for (DependencyRequest<K> dcm : waitingDepChecks.get(content.getKey())) {
                Occurred compare = order.compareMessages(dcm.getDependency().getVersion(), metaData.getVersion());
                if (compare == Occurred.AFTER || compare == Occurred.TIE) {
                //if (dcm.getDependency().getVersion().getTimestamp() <= metaData.getVersion().getTimestamp()) {
                    MetaData md = new MetaData();
                    HashMap<String, Version> dependency = new HashMap<>();
                    Version version = Configurations.getVersionObject();
                    //version.updateVersion(groupMembership.getNodeId(), dcm.getDependency().getVersion().getTimestamp(), -1);
                    for (Map.Entry<String, Long> entry : dcm.getDependency().getVersion().getVersions().entrySet()){
                        version.updateVersion( entry.getKey(), entry.getValue(), System.currentTimeMillis());
                    }
                    dependency.put((String) dcm.getDependency().getKey(), version);
                    md.setDependencies(dependency);
                    md.setNodeToSendResponse(metaData.getNodeToSendResponse());
                    toAnswer.add(new Message<K, V>(Message.Type.DEPENDENCY_RESPONSE, new Content<>(content.getKey()), md));
                }
            }
        }
        return toAnswer;
    }

    public boolean localDepChecking(Content<K, V> content, MetaData metaData){
        List<Dependency<K>> nearestList = new ArrayList<>();
        for (Map.Entry<String, Version> dependency : metaData.getDependencies().entrySet()){
            nearestList.add(new Dependency<>((K) dependency.getKey(), dependency.getValue()));
        }
        synchronized (pendingKeys) {
            List<Dependency<K>> remainingDeps = localDepChecking(content.getKey(), nearestList);
            if (remainingDeps.isEmpty()) {
                return true;
            } else {
                HashMap<String, Version> dependencies = new HashMap<>();
                for (Dependency<K> dep : remainingDeps){
                    Version version = Configurations.getVersionObject();
                    //version.updateVersion(groupMembership.getNodeId(), dep.getVersion().getTimestamp(), -1);
                    for (Map.Entry<String, Long> entry : dep.getVersion().getVersions().entrySet()){
                        version.updateVersion(entry.getKey(), entry.getValue(), System.currentTimeMillis());
                    }
                    dependencies.put((String) dep.getKey(), version);
                }
                metaData.setRemainingDependencies(dependencies);
                return false;
            }
        }
    }

    public List<Dependency<K>> localDepChecking(K key, List<Dependency<K>> deps) {
        List<Dependency<K>> remaining = new ArrayList<>();
        try {
            if (deps != null) {
                for (Dependency<K> dep : deps) {
                    int hostingPartition = 0;
                    hostingPartition = groupMembership.findPartition((String) dep.getKey());
                    if (hostingPartition != groupMembership.getPartId()) {
                        remaining.add(dep);
                    } else {
                        Version actual = communicationLocal.getActualVersion(new Content<K,V>(dep.getKey(), null), new MetaData());
                        //if (actual == null || actual.getTimestamp() < dep.getVersion().getTimestamp()) {
                        if (actual == null || order.compareMessages(actual, dep.getVersion()) == Occurred.AFTER)  {
                            remaining.add(dep);
                            addToWaitingLocalDeps(dep.getKey(), key);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println(("Error in local dep checking: " + e));
        }
        return remaining;
    }

    public void putPendingKeys(Content<K, V> content, MetaData metaData){
        List<Dependency<K>> nearestList = new ArrayList<>();
        for (Map.Entry<String, Version> dependency : metaData.getDependencies().entrySet()){
            nearestList.add(new Dependency<K>((K) dependency.getKey(), dependency.getValue()));
        }
        pendingKeys.put(content.getKey(), new PendingMessage<K,V>(content.getValue(), metaData.getVersion(), nearestList));
    }

}
