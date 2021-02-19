package edu.msu.cse.cops.server.consistency.versioning;

import edu.msu.cse.cops.server.consistency.exception.ConsistencyException;

public class InvalidClockEntryException extends ConsistencyException {

    private static final long serialVersionUID = 1L;

    public InvalidClockEntryException() {
        super();
    }

    public InvalidClockEntryException(String s, Throwable t) {
        super(s, t);
    }

    public InvalidClockEntryException(String s) {
        super(s);
    }

    public InvalidClockEntryException(Throwable t) {
        super(t);
    }
}
