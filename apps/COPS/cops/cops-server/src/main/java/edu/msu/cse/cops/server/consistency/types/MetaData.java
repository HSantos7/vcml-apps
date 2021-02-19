package edu.msu.cse.cops.server.consistency.types;

import edu.msu.cse.cops.server.consistency.Configurations;
import edu.msu.cse.cops.server.consistency.cluster.Node;
import edu.msu.cse.cops.server.consistency.utils.ByteArray;
import edu.msu.cse.cops.server.consistency.utils.pipeline.Response;
import edu.msu.cse.cops.server.consistency.versioning.Version;
import edu.msu.cse.cops.server.consistency.versioning.Versioned;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


public class MetaData {
    private Version version;
    private Versioned<?> versioned;
    private Node master;
    HashSet<Integer> zoneResponses;
    List<Response<ByteArray, Object>> responses;
    long startTimeNs;
    int getSuccesses;
    int putSuccesses;
    int replicateSuccesses;

    HashMap<String, Version> dependencies;
    HashMap<String, Version> remainingDependencies;

    Object clientMessageAgent;
    Node nodeToSendResponse;

    public MetaData() {
        try {
            this.version = (Version) Configurations.getVersionType().newInstance();
            this.getSuccesses = 0;
            this.responses = new ArrayList<Response<ByteArray, Object>>();
            this.zoneResponses = new HashSet<Integer>();
            this.dependencies = new HashMap<>();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public MetaData(Version version) {
        this();
        this.version = version;
    }

    public Version getVersion() {
        return version;
    }

    public Node getMaster() {
        return master;
    }

    public void setMaster(Node master) {
        this.master = master;
    }

    public Versioned<?> getVersioned() {
        return versioned;
    }

    public void setVersioned(Versioned<?> versioned) {
        this.versioned = versioned;
    }

    public HashSet<Integer> getZoneResponses() {
        return zoneResponses;
    }

    public void addZoneResponses(int zone) {
        this.zoneResponses.add(zone);
    }

    public long getStartTimeNs() {
        return startTimeNs;
    }

    public void setStartTimeNs(long startTimeNs) {
        this.startTimeNs = startTimeNs;
    }

    public List<Response<ByteArray, Object>> getResponses() {
        return responses;
    }

    public void setResponses(List<Response<ByteArray, Object>> responses) {
        this.responses = responses;
    }
    public Object getClientMessageAgent() {
        return clientMessageAgent;
    }

    public void setClientMessageAgent(Object clientMessageAgent) {
        this.clientMessageAgent = clientMessageAgent;
    }

    public void setVersion(Version version) {
        this.version = version;
    }

    public HashMap<String, Version> getDependencies() {
        return dependencies;
    }

    public void setDependencies(HashMap<String, Version> dependencies) {
        this.dependencies = dependencies;
    }

    public HashMap<String, Version> getRemainingDependencies() {
        return remainingDependencies;
    }

    public void setRemainingDependencies(HashMap<String, Version> remainingDependencies) {
        this.remainingDependencies = remainingDependencies;
    }

    public Node getNodeToSendResponse() {
        return nodeToSendResponse;
    }

    public void setNodeToSendResponse(Node nodeToSendResponse) {
        this.nodeToSendResponse = nodeToSendResponse;
    }

    public int getGetSuccesses() {
        return getSuccesses;
    }

    public void incrementGetSuccesses() {
        this.getSuccesses++;
    }

    public int getPutSuccesses() {
        return putSuccesses;
    }

    public void incrementPutSuccesses() {
        this.putSuccesses++;
    }

    public int getReplicateSuccesses() {
        return replicateSuccesses;
    }

    public void incrementReplicateSuccesses() {
        this.replicateSuccesses++;
    }

    public void clearZoneResponses() {
        this.zoneResponses.clear();
    }
}
