package voldemort.consistency;

import voldemort.consistency.types.Content;
import voldemort.consistency.types.MetaData;
import voldemort.consistency.versioning.Occurred;
import voldemort.consistency.versioning.Version;

import java.util.Map;

public interface OrderInterface<K,V> {
    MetaData timeStamping(Content<K,V> content, long startTime);
    Occurred compareMessages(Version ver1, Version ver2);
    void updateClock();
    void updateClock(Map<String,Long> newVersion);
}
