package edu.msu.cse.cops.server.consistency.types;

import edu.msu.cse.cops.server.consistency.versioning.Version;

import java.util.List;

public class PendingMessage<K,V> {
    V value;
    Version version;
    private final List<Dependency<K>> dependencies;


    public PendingMessage(V value, Version version, List<Dependency<K>> deps) {
        this.dependencies = deps;
        this.value = value;
        this.version = version;
    }

    public V getValue() {
        return value;
    }

    public Version getVersion() {
        return version;
    }

    public List<Dependency<K>> getDependencies() {
        return dependencies;
    }
}

