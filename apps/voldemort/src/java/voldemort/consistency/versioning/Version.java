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

package voldemort.consistency.versioning;

import java.util.Map;

/**
 * An interface that allows us to determine if a given version happened before
 * or after another version.
 * 
 * This could have been done using the comparable interface but that is
 * confusing, because the numeric codes are easily confused, and because
 * concurrent versions are not necessarily "equal" in the normal sense.
 * 
 * 
 */
public interface Version {

    /**
     * Return whether or not the given version preceded this one, succeeded it,
     * or is concurrent with it
     * 
     * @param v The other version
     */
    public Occurred compare(Version v);

    public Version clone();

    /**
     * Get new version object based on this clock but incremented on index nodeId
     *
     * @param nodeId The id of the node to increment
     * @return A version object equal on each element execept that indexed by
     *         nodeId
     */
    Version merge(Version clock);
    Version incremented(int nodeId, long time);
    long getTimestamp();
    void updateVersion(String nodeId, long newVersion, long currentTimeMillis);
    Map<String,Long> getVersions();

    int sizeInBytes();
    byte[] toBytes();
    void incrementVersion(int nodeId, long currentTimeMillis);

    int toBytes(byte[] bytes, int pos);
}
