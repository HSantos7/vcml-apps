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

import com.google.common.base.Objects;
import edu.msu.cse.cops.server.consistency.Configurations;
import edu.msu.cse.cops.server.consistency.utils.Utils;

import java.io.Serializable;
import java.util.Comparator;

/**
 * A wrapper for an object that adds a Version.
 * 
 * This class it bad to use in map, as the hashCode calls object.hashCode Most
 * likely the object is byte array and hashCode for array will be different for
 * different objects though the contents are the same
 */
public final class Versioned<T> implements Serializable {

    private static final long serialVersionUID = 1;

    private Version version;
    private volatile T object;

    public Versioned(T object) throws IllegalAccessException, InstantiationException {
        this(object, (Version) Configurations.getVersionType().newInstance());
    }

    public Versioned(T object, Version version) {
        if (version==null) {
            try {
                this.version = (Version) Configurations.getVersionType().newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            } else
                this.version = version;
        this.object = object;
    }

    public Version getVersion() {
        return version;
    }

    public T getValue() {
        return object;
    }

    public void setObject(T object) {
        this.object = object;
    }

    @Override
    public boolean equals(Object o) {
        if(o == this)
            return true;
        else if(!(o instanceof Versioned<?>))
            return false;

        Versioned<?> versioned = (Versioned<?>) o;
        return Objects.equal(getVersion(), versioned.getVersion())
               && Utils.deepEquals(getValue(), versioned.getValue());
    }

    @Override
    public int hashCode() {
        int value = 31 + version.hashCode();
        if(object != null) {
            // So two different arrays having same content will return two
            // different has codes
            value += 31 * object.hashCode();
        }
        return value;
    }

    @Override
    public String toString() {
        return "[" + object + ", " + version + "]";
    }

    /**
     * Create a clone of this Versioned object such that the object pointed to
     * is the same, but the VectorClock and Versioned wrapper is a shallow copy.
     */
    public Versioned<T> cloneVersioned() {
        return new Versioned<T>(this.getValue(), this.version.clone());
    }

    public static <S> Versioned<S> value(S s) {
        Version aux = null;
        try {
            aux = (Version) Configurations.getVersionType().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return new Versioned<S>(s, aux);
    }

    public static <S> Versioned<S> value(S s, Version v) {
        return new Versioned<S>(s, v);
    }

    public static final class HappenedBeforeComparator<S> implements Comparator<Versioned<S>> {

        public int compare(Versioned<S> v1, Versioned<S> v2) {
            Occurred occurred = v1.getVersion().compare(v2.getVersion());
            if(occurred == Occurred.BEFORE)
                return -1;
            else if(occurred == Occurred.AFTER)
                return 1;
            else
                return 0;
        }
    }

}
