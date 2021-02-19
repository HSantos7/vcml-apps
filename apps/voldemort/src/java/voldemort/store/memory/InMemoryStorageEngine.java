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

package voldemort.store.memory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.StoreUtils;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.consistency.versioning.ObsoleteVersionException;
import voldemort.consistency.versioning.Occurred;
import voldemort.consistency.versioning.Version;
import voldemort.consistency.versioning.Versioned;

/**
 * A simple non-persistent, in-memory store. Useful for unit testing.
 * 
 * TODO Rewrite this class using striped locks for more granular locking.
 * 
 */
public class InMemoryStorageEngine<K, V, T> extends AbstractStorageEngine<K, V, T> {

    private static final Logger logger = Logger.getLogger(InMemoryStorageEngine.class);
    protected final ConcurrentMap<K, List<Versioned<V>>> map;

    public InMemoryStorageEngine(String name) {
        super(name);
        this.map = new ConcurrentHashMap<K, List<Versioned<V>>>();
    }

    public InMemoryStorageEngine(String name, ConcurrentMap<K, List<Versioned<V>>> map) {
        super(name);
        this.map = Utils.notNull(map);
    }

    public synchronized void deleteAll() {
        this.map.clear();
    }

    public boolean delete(K key) {
        return delete(key, null);
    }

    @Override
    public synchronized boolean delete(K key, Version version) {
        StoreUtils.assertValidKey(key);

        List<Versioned<V>> values = map.get(key);
        if(values == null) {
            return false;
        }

        if(version == null) {
            map.remove(key);
            return true;
        }

        boolean deletedSomething = false;
        Iterator<Versioned<V>> iterator = values.iterator();
        while(iterator.hasNext()) {
            Versioned<V> item = iterator.next();
            if(item.getVersion().compare(version) == Occurred.BEFORE) {
                iterator.remove();
                deletedSomething = true;
            }
        }
        if(values.size() == 0) {
            // if there are no more versions left, also remove the key from the
            // map
            map.remove(key);
        }

        return deletedSomething;
    }

    @Override
    public List<Version> getVersions(K key) {
        return StoreUtils.getVersions(get(key, null));
    }

    @Override
    public synchronized List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        List<Versioned<V>> results = map.get(key);
        if(results == null) {
            return new ArrayList<Versioned<V>>(0);
        } else {
            return new ArrayList<Versioned<V>>(results);
        }
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        return StoreUtils.getAll(this, keys, transforms);
    }

    @Override
    public synchronized void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        List<Versioned<V>> items = map.get(key);
        // If we have no value, add the current value
        if(items == null) {
            items = new ArrayList<Versioned<V>>();
        }
        // Check for existing versions - remember which items to
        // remove in case of success
        List<Versioned<V>> itemsToRemove = new ArrayList<Versioned<V>>(items.size());
        for(Versioned<V> versioned: items) {
            Occurred occurred = value.getVersion().compare(versioned.getVersion());
            if(occurred == Occurred.BEFORE) {
                throw new ObsoleteVersionException("Obsolete version for key '" + key + "': "
                                                   + value.getVersion());
            } else if(occurred == Occurred.AFTER) {
                itemsToRemove.add(versioned);
            }
        }
        items.removeAll(itemsToRemove);
        items.add(value);
        map.put(key, items);
    }

    @Override
    public synchronized List<Versioned<V>> multiVersionPut(K key, final List<Versioned<V>> values) {
        // TODO the day this class implements getAndLock and putAndUnlock, this
        // method can be removed
        StoreUtils.assertValidKey(key);
        List<Versioned<V>> obsoleteVals = null;
        List<Versioned<V>> valuesInStorage = null;
        valuesInStorage = map.get(key);
        if(valuesInStorage == null) {
            valuesInStorage = new ArrayList<Versioned<V>>(values.size());
        }
        obsoleteVals = resolveAndConstructVersionsToPersist(valuesInStorage, values);
        map.put(key, valuesInStorage);
        return obsoleteVals;
    }

    @Override
    public ClosableIterator<Pair<K, Versioned<V>>> entries() {
        return new InMemoryIterator<K, V, T>(map, this);
    }

    @Override
    public ClosableIterator<K> keys() {
        // TODO Implement more efficient version.
        return StoreUtils.keys(entries());
    }

    @Override
    public ClosableIterator<Pair<K, Versioned<V>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    @Override
    public ClosableIterator<K> keys(int partition) {
        throw new UnsupportedOperationException("Partition based key scan not supported for this storage type");
    }

    @Override
    public synchronized void truncate() {
        map.clear();
    }

    @Override
    public String toString() {
        return toString(15);
    }

    public String toString(int size) {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        int count = 0;
        for(Entry<K, List<Versioned<V>>> entry: map.entrySet()) {
            if(count > size) {
                builder.append("...");
                break;
            }
            builder.append(entry.getKey());
            builder.append(':');
            builder.append(entry.getValue());
            builder.append(',');
        }
        builder.append('}');
        return builder.toString();
    }

    /**
     * This class relies on the concurrent hash map's iterator to return a
     * weakly consistent view of the data in the map.
     */
    @NotThreadsafe
    private static class InMemoryIterator<K, V, T> implements
            ClosableIterator<Pair<K, Versioned<V>>> {

        private final Iterator<Entry<K, List<Versioned<V>>>> iterator;
        private K currentKey;
        private Iterator<Versioned<V>> currentValues;
        private InMemoryStorageEngine<K, V, T> inMemoryStorageEngine;

        public InMemoryIterator(ConcurrentMap<K, List<Versioned<V>>> map,
                                InMemoryStorageEngine<K, V, T> inMemoryStorageEngine) {
            this.iterator = map.entrySet().iterator();
            this.inMemoryStorageEngine = inMemoryStorageEngine;
        }

        @Override
        public boolean hasNext() {
            return hasNextInCurrentValues() || iterator.hasNext();
        }

        private boolean hasNextInCurrentValues() {
            return currentValues != null && currentValues.hasNext();
        }

        private Pair<K, Versioned<V>> nextInCurrentValues() {
            Versioned<V> item = currentValues.next();
            return Pair.create(currentKey, item);
        }

        @Override
        public Pair<K, Versioned<V>> next() {
            if(hasNextInCurrentValues()) {
                return nextInCurrentValues();
            } else {
                // keep trying to get a next, until we find one (they could get
                // removed)
                while(true) {
                    Entry<K, List<Versioned<V>>> entry = iterator.next();

                    List<Versioned<V>> list = entry.getValue();
                    synchronized(this.inMemoryStorageEngine) {
                        // okay we may have gotten an empty list, if so try
                        // again
                        if(list.size() == 0)
                            continue;

                        // grab a snapshot of the list while we have exclusive
                        // access
                        currentValues = new ArrayList<Versioned<V>>(list).iterator();
                    }
                    currentKey = entry.getKey();
                    return nextInCurrentValues();
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("No removal y'all.");
        }

        @Override
        public void close() {
            // nothing to do here
        }
    }
}
