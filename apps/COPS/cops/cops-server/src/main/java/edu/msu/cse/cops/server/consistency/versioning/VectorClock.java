/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package edu.msu.cse.cops.server.consistency.versioning;

import com.google.common.collect.Maps;
import edu.msu.cse.cops.server.consistency.Configurations;
import edu.msu.cse.cops.server.consistency.utils.ByteUtils;
import edu.msu.cse.cops.server.consistency.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.TreeMap;

/**
 * A vector of the number of writes mastered by each node. The vector is stored
 * sparely, since, in general, writes will be mastered by only one node. This
 * means implicitly all the versions are at zero, but we only actually store
 * those greater than zero.
 * 
 * 
 */
//NOT THREAD SAFE
public class VectorClock implements Version, Serializable {

    private static final long serialVersionUID = 1;

    private static final int MAX_NUMBER_OF_VERSIONS = Short.MAX_VALUE;

    /* A map of versions keyed by nodeId */
    private final TreeMap<String, Long> versionMap;

    /*
     * The time of the last update on the server on which the update was
     * performed
     */
    private volatile long timestamp;

    /**
     * Construct an empty VectorClock
     */
    public VectorClock() {
        this(System.currentTimeMillis());
    }

    public TreeMap<String, Long> getVersionMap() {
        return versionMap;
    }

    public VectorClock(long timestamp) {
        this.versionMap = new TreeMap<String, Long>();
        this.timestamp = timestamp;
    }

    /**
     * Only used for cloning
     * 
     * @param versionMap
     * @param timestamp
     */
    private VectorClock(TreeMap<String, Long> versionMap, long timestamp) {
        this.versionMap = Utils.notNull(versionMap);
        this.timestamp = timestamp;
    }

    public static Version createNew(DataInputStream inputStream) {
        try {
            final int HEADER_LENGTH = ByteUtils.SIZE_OF_SHORT + ByteUtils.SIZE_OF_BYTE;
            byte[] header = new byte[HEADER_LENGTH];
            inputStream.readFully(header);
            int numEntries = ByteUtils.readShort(header, 0);

            byte versionSize = header[ByteUtils.SIZE_OF_SHORT];

            int entrySize = ByteUtils.SIZE_OF_SHORT + versionSize;
            int totalEntrySize = numEntries * entrySize;

            byte[] vectorClockBytes = new byte[HEADER_LENGTH + totalEntrySize
                                               + ByteUtils.SIZE_OF_LONG];
            System.arraycopy(header, 0, vectorClockBytes, 0, header.length);

            inputStream.readFully(vectorClockBytes, HEADER_LENGTH, vectorClockBytes.length
                                                                   - HEADER_LENGTH);

            try {
                Constructor constructor = Configurations.getVersionType().getConstructor(vectorClockBytes.getClass());
                return (Version) constructor.newInstance( vectorClockBytes);
            } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
                e.printStackTrace();
            }
        } catch(IOException e) {
            throw new IllegalArgumentException("Can't deserialize vectorclock from stream", e);
        }
        return null;
    }

    public int sizeInBytes() {
        byte versionSize = ByteUtils.numberOfBytesRequired(getMaxVersion());
        return ByteUtils.SIZE_OF_SHORT + 1 + this.versionMap.size()
               * (ByteUtils.SIZE_OF_SHORT + versionSize) + ByteUtils.SIZE_OF_LONG;
    }

    /**
     * Increment the version info associated with the given node
     * 
     * @param node The node
     */
    public void incrementVersion(String node, long time) {
        if(node.equals(""))
            throw new IllegalArgumentException(node
                                               + " is outside the acceptable range of node ids.");

        this.timestamp = time;

        Long version = versionMap.get(node);
        if(version == null) {
            version = 1L;
        } else {
            version = version + 1L;
        }

        versionMap.put(node, version);
        if(versionMap.size() >= MAX_NUMBER_OF_VERSIONS) {
            throw new IllegalStateException("Vector clock is full!");
        }

    }

    @Override
    public void updateVersion(String nodeId, long newVersion, long currentTimeMillis) {
        if(nodeId.equals(""))
            throw new IllegalArgumentException(nodeId
                    + " is outside the acceptable range of node ids.");

        this.timestamp = currentTimeMillis;
        synchronized (versionMap){
            versionMap.put(nodeId, newVersion);
            if(versionMap.size() >= MAX_NUMBER_OF_VERSIONS) {
                throw new IllegalStateException("Vector clock is full!");
            }
        }

    }

    @Override
    public Map<String, Long> getVersions() {
        return versionMap;
    }


    @Override
    public VectorClock clone() {
        return new VectorClock(Maps.newTreeMap(versionMap), this.timestamp);
    }

    @Override
    public boolean equals(Object object) {
        if(this == object)
            return true;
        if(object == null)
            return false;
        if(!object.getClass().equals(VectorClock.class))
            return false;
        VectorClock clock = (VectorClock) object;
        return versionMap.equals(clock.versionMap);
    }

    @Override
    public int hashCode() {
        return versionMap.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("version(");
        int versionsLeft = versionMap.size();
        for(Map.Entry<String, Long> entry: versionMap.entrySet()) {
            versionsLeft--;
            String node = entry.getKey();
            Long version = entry.getValue();
            builder.append(node).append(":").append(version);
            if(versionsLeft > 0) {
                builder.append(", ");
            }
        }
        builder.append(")");
        builder.append(" ts:").append(timestamp);
        return builder.toString();
    }

    public long getMaxVersion() {
        long max = -1;
        for(Long version: versionMap.values())
            max = Math.max(version, max);
        return max;
    }

    @Override
    public Version merge(Version clock) {
        VectorClock newClock = new VectorClock();
        for(Map.Entry<String, Long> entry: this.versionMap.entrySet()) {
            newClock.versionMap.put(entry.getKey(), entry.getValue());
        }
        for(Map.Entry<String, Long> entry: ((VectorClock) clock).versionMap.entrySet()) {
            Long version = newClock.versionMap.get(entry.getKey());
            if(version == null) {
                newClock.versionMap.put(entry.getKey(), entry.getValue());
            } else {
                newClock.versionMap.put(entry.getKey(), Math.max(version, entry.getValue()));
            }
        }

        return newClock;
    }

    @Override
    public Occurred compare(Version v) {
        if(!(v instanceof VectorClock))
            throw new IllegalArgumentException("Cannot compare Versions of different types.");

        return VectorClockUtils.compare(this, (VectorClock) v);
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

}
