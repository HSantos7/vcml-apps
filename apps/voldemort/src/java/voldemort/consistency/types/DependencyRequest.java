package voldemort.consistency.types;

import voldemort.consistency.cluster.Node;
import voldemort.consistency.versioning.Version;

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

