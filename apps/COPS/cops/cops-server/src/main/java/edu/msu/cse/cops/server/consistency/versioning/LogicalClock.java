package edu.msu.cse.cops.server.consistency.versioning;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class implements logical clock abstraction. Logical clock is a mechanism for capturing
 * chronological and causal relationships between events in a distributed system.
 *
 * <p>
 * This implementation provides methods to store local timestamp and update it in a thread safe and
 * non-blocking way.
 *
 * @author Anton Kharenko
 * @see LogicalTimestamp
 */
public class LogicalClock implements Version{

    private final AtomicReference<LogicalTimestamp> timeReference = new AtomicReference<>();

    /**
     * Creates instance of logical clock with default initial timestamp.
     */
    public LogicalClock() {
        this(new LogicalTimestamp());
    }

    public LogicalClock(byte[] bytes) {
        this(LogicalTimestamp.fromBytes(bytes));
    }

    /**
     * Creates instance of logical clock with the given initial timestamp.
     */
    public LogicalClock(LogicalTimestamp initialTimestamp) {
        this.timeReference.set(initialTimestamp);
    }

    /**
     * Returns current value of the clock.
     */
    public LogicalTimestamp time() {
        return timeReference.get();
    }

    /**
     * Increments the clock time and returns newly set value of the clock.
     *
     * @return New value of the clock.
     */
    public void incrementVersion() {
        LogicalTimestamp previousTimestamp, nextTimestamp;
        do {
            previousTimestamp = timeReference.get();
            nextTimestamp = previousTimestamp.nextTimestamp();
        } while (!timeReference.compareAndSet(previousTimestamp, nextTimestamp));
    }

    /**
     * Increments the value of the clock taking into account that provided timestamp happens before
     * that moment. Returns new value of the clock which happens after previous value of the clock and
     * provided timestamp.
     *
     * @param happensBeforeTimestamp timestamp value which happens in the past
     * @return New value of the clock.
     */
    public LogicalTimestamp tick(LogicalTimestamp happensBeforeTimestamp) {
        LogicalTimestamp previousTimestamp, nextTimestamp;
        do {
            previousTimestamp = timeReference.get();
            if (previousTimestamp.isAfter(happensBeforeTimestamp)) {
                nextTimestamp = previousTimestamp.nextTimestamp();
            } else {
                nextTimestamp = happensBeforeTimestamp.nextTimestamp();
            }
        } while (!timeReference.compareAndSet(previousTimestamp, nextTimestamp));
        return nextTimestamp;
    }

    @Override
    public Occurred compare(Version v) {
        if(!(v instanceof LogicalClock))
            throw new IllegalArgumentException("Cannot compare Versions of different types.");
        int compare = this.time().compareTo(((LogicalClock) v).time());
        if (compare < 0)
            return Occurred.BEFORE;
        else if (compare > 0)
            return Occurred.AFTER;
        else
            return Occurred.CONCURRENTLY;
    }

    @Override
    public LogicalClock clone() {
        return new LogicalClock(this.time());
    }

    @Override
    public Version merge(Version clock) {
        return null;
    }

    @Override
    public void incrementVersion(String nodeId, long currentTimeMillis) {
        incrementVersion();
    }

    @Override
    public void updateVersion(String nodeId, long newVersion, long currentTimeMillis) {

    }

    @Override
    public Map<String, Long> getVersions() {
        return null;
    }

    @Override
    public Long getTimestamp() {
        return time().toLong();
    }


}
