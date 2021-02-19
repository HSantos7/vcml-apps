package voldemort.store.memory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.store.StoreUtils;
import voldemort.consistency.versioning.Versioned;

/**
 * . This class is used to assert puts on keys and to examine what key put
 * assertions has been fulfilled and what are not This is particularly useful
 * for cases where there are a large number of puts and the their values do not
 * matter and they are not read
 * 
 * @param <K> Key Type
 * @param <V> Value Type
 * @param <T> Transformation Type
 */
public class InMemoryPutAssertionStorageEngine<K, V, T> extends InMemoryStorageEngine<K, V, T> {

    private static final Logger logger = Logger.getLogger(InMemoryPutAssertionStorageEngine.class);

    private final ConcurrentMap<K, Boolean> assertionMap;

    public InMemoryPutAssertionStorageEngine(String name) {
        super(name);
        this.assertionMap = new ConcurrentHashMap<K, Boolean>();
    }

    public synchronized void assertPut(K key) throws VoldemortException {
        StoreUtils.assertValidKey(key);

        // delete if exist
        List<Versioned<V>> result = map.remove(key);
        if(result == null || result.size() == 0) {
            // if non-exist, record as assertion
            assertionMap.put(key, true); // use synchronized to avoid race
                                         // condition here
            if(logger.isDebugEnabled()) {
                logger.debug("PUT Assertion added (not yet fulfilled) for key: " + key
                             + " assertionMap size: " + assertionMap.size());
            }
        } else {
            if(logger.isTraceEnabled()) {
                logger.trace("PUT Assertion added (immediately fulfilled) for key: " + key
                             + " assertionMap size: " + assertionMap.size());
            }
        }
    }

    @Override
    public synchronized void put(K key, Versioned<V> value, T transforms) throws VoldemortException {
        // try to delete from assertion
        // do real put if has not been asserted
        Boolean result = assertionMap.remove(key);

        if(result == null) {

            logger.info("PUT key: " + key + " (never asserted) assertionMap size: "
                        + assertionMap.size());
            super.put(key, value, transforms);
            if(logger.isTraceEnabled()) {
                logger.trace("PUT key: " + key + " (never asserted) assertionMap size: "
                             + assertionMap.size());
            }
        } else {
            logger.info("PUT key: " + key
                        + " (found and fulfills put assertion) assertionMap size: "
                        + assertionMap.size());
            if(logger.isDebugEnabled()) {
                logger.debug("PUT key: " + key
                             + " (found and fulfills put assertion) assertionMap size: "
                             + assertionMap.size());
            }
        }
    }

    @Override
    public synchronized List<Versioned<V>> multiVersionPut(K key, final List<Versioned<V>> values) {
        Boolean result = assertionMap.remove(key);
        if(result == null) {
            if(logger.isTraceEnabled()) {
                logger.trace("PUT key: " + key + " (never asserted) assertionMap size: "
                             + assertionMap.size());
            }
        } else {
            if(logger.isDebugEnabled()) {
                logger.debug("PUT key: " + key
                             + " (found and fulfills put assertion) assertionMap size: "
                             + assertionMap.size());
            }
        }
        List<Versioned<V>> obsoleteVals = super.multiVersionPut(key, values);
        return obsoleteVals;
    }

    public Set<K> getFailedAssertions() {
        return Collections.unmodifiableSet(assertionMap.keySet());
    }
}
