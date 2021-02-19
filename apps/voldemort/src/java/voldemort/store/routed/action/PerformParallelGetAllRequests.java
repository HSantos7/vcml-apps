/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.routed.action;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Level;

import voldemort.consistency.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.consistency.Callback;
import voldemort.store.InvalidMetadataException;
import voldemort.store.nonblockingstore.NonblockingStore;
import voldemort.store.routed.GetAllPipelineData;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.Pipeline.Event;
import voldemort.consistency.utils.pipeline.Response;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.versioning.Versioned;

import com.google.common.collect.Lists;

public class PerformParallelGetAllRequests
        extends
        AbstractAction<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>, GetAllPipelineData> {

    private final long timeoutMs;

    private final Map<Integer, NonblockingStore> nonblockingStores;

    private final FailureDetector failureDetector;

    public PerformParallelGetAllRequests(GetAllPipelineData pipelineData,
                                         Event completeEvent,
                                         FailureDetector failureDetector,
                                         long timeoutMs,
                                         Map<Integer, NonblockingStore> nonblockingStores) {
        super(pipelineData, completeEvent);
        this.failureDetector = failureDetector;
        this.timeoutMs = timeoutMs;
        this.nonblockingStores = nonblockingStores;
    }

    @SuppressWarnings("unchecked")
    public void execute(final Pipeline pipeline) {
        int attempts = pipelineData.getNodeToKeysMap().size();
        final Map<Integer, Response<Iterable<ByteArray>, Object>> responses = new ConcurrentHashMap<Integer, Response<Iterable<ByteArray>, Object>>();
        final CountDownLatch latch = new CountDownLatch(attempts);

        if (logger.isTraceEnabled())
            logger.trace("Attempting " + attempts + " " + pipeline.getOperation().getSimpleName()
                         + " operations in parallel");

        Map<ByteArray, byte[]> transforms = pipelineData.getTransforms();

        final AtomicBoolean isResponseProcessed = new AtomicBoolean(false);

        for (Map.Entry<Node, List<ByteArray>> entry: pipelineData.getNodeToKeysMap().entrySet()) {
            final Node node = entry.getKey();
            final Collection<ByteArray> keys = entry.getValue();

            Callback callback = new Callback() {

                public void requestComplete(Object result, long requestTime) {
                    if (logger.isTraceEnabled())
                        logger.trace(pipeline.getOperation().getSimpleName()
                                     + " response received (" + requestTime + " ms.) from node "
                                     + node.getId());

                    Response<Iterable<ByteArray>, Object> response = new Response<Iterable<ByteArray>, Object>(node,
                                                                                                               keys,
                                                                                                               result,
                                                                                                               requestTime);
                    responses.put(node.getId(), response);
                    latch.countDown();
                    // TODO: There is inconsistency between the exceptions are treated here in the
                    // completion callback and in the application thread.
                    // They need a cleanup to make them consistent. Thought about handling
                    // them here, but it is the selector thread that is calling this completion method,
                    // handleResponseError has some synchronization, not sure about the effect, so reserving
                    // it for later.

                    // isResponseProcessed just reduces the time window during
                    // which a exception can go uncounted. When the parallel
                    // requests timeout and it is trying Serial timeout
                    // exceptions are lost and the node is never marked down.
                    // This reduces the window where an exception is lost
                    if (isResponseProcessed.get() && response.getValue() instanceof Exception)
                        if (response.getValue() instanceof InvalidMetadataException) {
                            pipelineData.reportException((InvalidMetadataException) response.getValue());
                            logger.warn("Received invalid metadata problem after a successful "
                                        + pipeline.getOperation().getSimpleName()
                                        + " call on node " + node.getId() + ", store '"
                                        + pipelineData.getStoreName() + "'");
                        } else {
                            handleResponseError(response, pipeline, failureDetector);
                        }
                }

            };

            if (logger.isTraceEnabled())
                logger.trace("Submitting " + pipeline.getOperation().getSimpleName()
                             + " request on node " + node.getId());

            NonblockingStore store = nonblockingStores.get(node.getId());
            store.submitGetAllRequest(keys, transforms, callback, timeoutMs);
        }

        try {
            latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            if (logger.isEnabledFor(Level.WARN))
                logger.warn(e, e);
        }

        for (Response<Iterable<ByteArray>, Object> response: responses.values()) {
            if (response.getValue() instanceof Exception) {
                if (handleResponseError(response, pipeline, failureDetector))
                    return;
            } else {
                Map<ByteArray, List<Versioned<byte[]>>> values = (Map<ByteArray, List<Versioned<byte[]>>>) response.getValue();

                for (ByteArray key: response.getKey()) {
                    MutableInt successCount = pipelineData.getSuccessCount(key);
                    successCount.increment();

                    List<Versioned<byte[]>> retrieved = values.get(key);
                    /*
                     * retrieved can be null if there are no values for the key
                     * provided
                     */
                    if (retrieved != null) {
                        List<Versioned<byte[]>> existing = pipelineData.getResult().get(key);

                        if (existing == null)
                            pipelineData.getResult().put(key, Lists.newArrayList(retrieved));
                        else
                            existing.addAll(retrieved);
                    }

                    HashSet<Integer> zoneResponses = null;
                    if (pipelineData.getKeyToZoneResponse().containsKey(key)) {
                        zoneResponses = pipelineData.getKeyToZoneResponse().get(key);
                    } else {
                        zoneResponses = new HashSet<Integer>();
                        pipelineData.getKeyToZoneResponse().put(key, zoneResponses);
                    }
                    zoneResponses.add(response.getNode().getZoneId());
                }

                pipelineData.getResponses()
                            .add(new Response<Iterable<ByteArray>, Map<ByteArray, List<Versioned<byte[]>>>>(response.getNode(),
                                                                                                            response.getKey(),
                                                                                                            values,
                                                                                                            response.getRequestTime()));
                failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
            }
        }
        isResponseProcessed.set(true);

        pipeline.addEvent(completeEvent);
    }
}