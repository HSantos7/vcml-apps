/*
 * Copyright 2009 Geir Magnusson Jr.
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

package voldemort.store.noop;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.consistency.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.consistency.versioning.Version;
import voldemort.consistency.versioning.Versioned;

/**
 * Implementation of a store that does the least amount possible. It will
 * 'reflect' values sent to it so that it can be tested with real values. It's
 * being done this way to avoid coupling the engine or it's configuration with
 * knowledge of the serializer being used
 * 
 */
public class NoopStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {

    protected boolean dataReflect;
    protected ByteArray key;
    protected Versioned<byte[]> value;
    protected List<Versioned<byte[]>> dataList = new MyList();
    protected Map<ByteArray, List<Versioned<byte[]>>> dataMap = new MyMap();

    public NoopStorageEngine(String name, boolean reflect) {
        super(name);
        this.dataReflect = reflect;
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        throw new UnsupportedOperationException("Partition based entries scan not supported for this storage type");
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        throw new UnsupportedOperationException("Partition based key scan not supported for this storage type");
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        return dataList;
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        return dataMap;
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key, null));
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {

        if(dataReflect) {
            this.key = key;
            this.value = value;
        }
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        return true;
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    class MyMap extends HashMap<ByteArray, List<Versioned<byte[]>>> {

        public static final long serialVersionUID = 1;

        @Override
        public List<Versioned<byte[]>> get(Object key) {
            return dataList;
        }
    }

    class MyList extends ArrayList<Versioned<byte[]>> {

        public static final long serialVersionUID = 1;

        @Override
        public Versioned<byte[]> get(int index) {
            return value;
        }

        @Override
        public int size() {
            return value == null ? 0 : 1;
        }
    }
}
