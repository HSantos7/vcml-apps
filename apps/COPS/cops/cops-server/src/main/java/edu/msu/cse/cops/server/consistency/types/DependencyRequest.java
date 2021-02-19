package edu.msu.cse.cops.server.consistency.types;

import edu.msu.cse.cops.server.consistency.cluster.Node;
import edu.msu.cse.cops.server.consistency.versioning.Version;

public class DependencyRequest<K> {
    private final Dependency<K> dependency;
    private final Node node;

    public DependencyRequest(K key, Version version, Node node) {
        this.dependency = new Dependency<K>(key, version);
        this.node = node;
    }

    public Dependency<K> getDependency() {
        return dependency;
    }

    public Node getNode() {
        return node;
    }
}

