package edu.msu.cse.cops.server.consistency.versioning;


import java.util.Map;
import java.util.TreeMap;

public class LongClock implements Version{
    long value;

    public LongClock() {
        this.value = 0L;
    }

    @Override
    public Occurred compare(Version v) {
        if(this.value > v.getVersions().entrySet().stream().findFirst().get().getValue())
            return Occurred.BEFORE;
        else if (this.value == v.getVersions().entrySet().stream().findFirst().get().getValue())
            return Occurred.TIE;
        return Occurred.AFTER;
    }

    @Override
    public Version clone() {
        return null;
    }

    @Override
    public Version merge(Version clock) {
        return null;
    }

    @Override
    public void incrementVersion(String nodeId, long currentTimeMillis) {
        this.value++;
    }


    @Override
    public void updateVersion(String nodeId, long newVersion, long currentTimeMillis) {
        this.value = newVersion;
    }

    @Override
    public Map<String, Long> getVersions() {
        Map<String,Long> map = new TreeMap<>();
        map.put("-1",value);
        return map;
    }

    @Override
    public Long getTimestamp() {
        return -1L;
    }
}
