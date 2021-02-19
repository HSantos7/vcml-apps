package edu.msu.cse.cops.server.consistency;

public interface Callback {
    /**
     * Signals that the request is complete and provides the response -- or
     * Exception (if the request failed) -- and the time (in milliseconds) it
     * took between issuing the request and receiving the response.
     *
     * @param result Type-specific result from the request or Exception if the
     *        request failed
     * @param requestTime Time (in milliseconds) for the duration of the request
     */

    public void requestComplete(Object result, long requestTime);
}
