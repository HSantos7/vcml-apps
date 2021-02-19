package voldemort.consistency.types;


import voldemort.consistency.versioning.Version;

public class Dependency<K> {
    private final K key;
    private Version version;

    public Dependency(K key, Version version) {
        this.key = key;
        this.version = version;
    }

    public K getKey() {
        return key;
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        this.version = version;
    }
}

