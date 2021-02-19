package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.types.Content;
import edu.msu.cse.cops.server.consistency.versioning.Occurred;
import edu.msu.cse.cops.server.consistency.versioning.Version;

import java.util.Map;

public interface OrderInterface<K,V> {
    Version timeStamping(Content<K,V> content, long startTime);
    Occurred compareMessages(Version ver1, Version ver2);
    void updateClock();
    void updateClock(Map<String,Long> newVersion);
    //Version getClock();
}
