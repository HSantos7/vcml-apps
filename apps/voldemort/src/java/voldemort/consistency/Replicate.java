package voldemort.consistency;

import voldemort.VoldemortClientShell;
import voldemort.consistency.cluster.Node;
import voldemort.consistency.types.Content;
import voldemort.consistency.types.Message;
import voldemort.consistency.types.Message.Type;
import voldemort.consistency.types.MetaData;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.utils.pipeline.Response;
import voldemort.consistency.versioning.ObsoleteVersionException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.quota.QuotaExceededException;
import voldemort.utils.Time;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class Replicate<K,V> implements ReplicateInterface<K,V> {
    GroupMembershipInterface groupMembership;
    QuorumInterface<K,V> quorum;
    CommunicationInterface.internal<K,V> communicationLocal;
    private Integer numNodesPendingResponse = 0;
    boolean quorumSatisfied = false;
    boolean zonesSatisfied = false;
    int numResponsesGot = 0;
    boolean responseHandlingCutoff = false;

    public Replicate(GroupMembershipInterface groupMembership, QuorumInterface<K,V> quorum, CommunicationInterface.internal<K,V> communicationLocal, CommunicationInterface.external<K,V> communicationRemote, DeliveryConditionInterface<K,V> deliveryCondition) {
        this.groupMembership = groupMembership;
        this.quorum = quorum;
        this.communicationLocal = communicationLocal;
    }

    @Override
    public void replicate(Message<K, V> message) {
        Content<K,V> content = message.getContent();
        MetaData metaData = message.getMetaData();

        final Queue<Response<ByteArray, Object>> responseQueue = new LinkedList<>();
        final Node masterNode = metaData.getMaster();
        final List<Node> nodes = groupMembership.getNodes(new ByteArray(VoldemortClientShell.serializeKey(content.getKey())),
                Constants.requiredWrites);
        final int numNodesTouchedInSerialPut = nodes.indexOf(masterNode) + 1;
        numNodesPendingResponse = nodes.size() - numNodesTouchedInSerialPut;

        System.out.println("PUT {key:" + content.getKey() + "} MasterNode={id:" + masterNode.getId()
                    + "} totalNodesToAsyncPut=" + numNodesPendingResponse);

        // initiate parallel puts
        for(int i = numNodesTouchedInSerialPut; i < nodes.size(); i++) {
            final Node node = nodes.get(i);
            Callback callback = new Callback() {

                @Override
                public void requestComplete(Object result, long requestTime) {
                    boolean responseHandledByMaster = false;
                    System.out.println("PUT {key:" + content.getKey() + "} response received from node={id:"
                                + node.getId() + "} in " + requestTime + " ms)");

                    Response<ByteArray, Object> response;
                    response = new Response<>(node, new ByteArray(VoldemortClientShell.serializeKey(content.getKey())), result, requestTime);

                    System.out.println("PUT {key:"
                                + content.getKey()
                                + "} Parallel put thread trying to return result to main thread");

                    if(!responseHandlingCutoff) {
                        responseQueue.offer(response);
                        this.notifyAll();
                        responseHandledByMaster = true;
                    }

                    System.out.println("PUT {key:" + content.getKey() + "} Master thread accepted the response: "+ responseHandledByMaster);


                    if(!responseHandledByMaster) {
                        System.out.println("PUT {key:"
                                    + content.getKey()
                                    + "} Master thread did not accept the response: will handle in worker thread");
                        }
                        if(response.getValue() instanceof QuotaExceededException) {


                            System.out.println("PUT {key:" + content.getKey() + "} failed on node={id:"
                                        + node.getId() + ",host:" + node.getHost() + "}");
/*
                            if(isHintedHandoffEnabled()) {
                                boolean triedDelegateSlop = pipelineData.getSynchronizer()
                                        .tryDelegateSlop(node);
                                if(logger.isDebugEnabled()) {
                                    logger.debug("PUT {key:" + key + "} triedDelegateSlop: "
                                            + triedDelegateSlop);
                                }
                                if(!triedDelegateSlop) {
                                    Slop slop = new Slop(pipelineData.getStoreName(),
                                            Slop.Operation.PUT,
                                            key,
                                            versionedCopy.getValue(),
                                            transforms,
                                            node.getId(),
                                            new Date());
                                    pipelineData.addFailedNode(node);
                                    if(logger.isDebugEnabled())
                                        logger.debug("PUT {key:" + key
                                                + "} Start registering Slop(node:"
                                                + node.getId() + ",host:" + node.getHost()
                                                + ")");
                                    hintedHandoff.sendHintParallel(node,
                                            versionedCopy.getVersion(),
                                            slop);
                                    if(logger.isDebugEnabled())
                                        logger.debug("PUT {key:" + key
                                                + "} Sent out request to register Slop(node: "
                                                + node.getId() + ",host:" + node.getHost()
                                                + ")");
                                }
                            }
                        } else {*/
                            // did not slop because either it's not exception or
                            // the exception is ignorable
                            //if(logger.isDebugEnabled()) {
                                if(result instanceof Exception) {
                                    System.out.println("PUT {key:"
                                            + content.getKey()
                                            + "} will not send hint. Response is ignorable exception: "
                                            + result.getClass().toString());
                                } else {
                                    System.out.println("PUT {key:" + content.getKey()
                                            + "} will not send hint. Response is success");
                                }
                            //}
                        }

                        if(result instanceof Exception
                                && !(result instanceof ObsoleteVersionException)) {
                            if(response.getValue() instanceof InvalidMetadataException) {
                                System.out.println("Received invalid metadata problem after a successful "
                                        + " call on node " + node.getId() + ", store '"
                                        +"'");
                            } else if(response.getValue() instanceof QuotaExceededException) {
                                /**
                                 * TODO Not sure if we need to count this
                                 * Exception for stats or silently ignore and
                                 * just log a warning. While
                                 * QuotaExceededException thrown from other
                                 * places mean the operation failed, this one
                                 * does not fail the operation but instead
                                 * stores slops. Introduce a new Exception in
                                 * client side to just monitor how mamy Async
                                 * writes fail on exceeding Quota?
                                 *
                                 */
                                System.out.println("ERROR");
                               //logger.warn("Received QuotaExceededException after a successful "
                                        //+ pipeline.getOperation().getSimpleName()
                                        //+ " call on node " + node.getId() + ", store '"
                                        //+ pipelineData.getStoreName() + "', master-node '"
                                        //+ masterNode.getId() + "'");
                            } else {
                                System.out.println("ERROR");
                                return;
                                //handleResponseError(response, pipeline, failureDetector);
                            }
                        }
                    }
            };

            System.out.println("Submitting" + " request on node " + node.getId() + " for key " + content.getKey());
            communicationLocal.put(node, content, metaData, callback);
            }

        try {
            boolean preferredSatisfied = false;
            while(true) {
                long elapsedNs = System.nanoTime() - metaData.getStartTimeNs();
                long remainingNs = (Constants.putOpTimeoutInMs * Time.NS_PER_MS) - elapsedNs;
                remainingNs = Math.max(0, remainingNs);
                // preferred check
                if(numResponsesGot >= Constants.preferredWrites - 1) {
                    preferredSatisfied = true;
                }

                quorumSatisfied = quorum.isZonesSatisfied(message);
                zonesSatisfied = quorum.isQuorumSatisfied(message);

                if(quorumSatisfied && zonesSatisfied && preferredSatisfied || remainingNs <= 0
                        || numNodesPendingResponse <= 0) {
                    responseHandlingCutoff = true;
                    break;
                } else {
                    System.out.println("PUT {key:" + content.getKey() + "} trying to poll from queue");

                    Response<ByteArray, Object> response;
                    try {
                        response = responseQueuePoll(responseQueue, remainingNs,
                                        TimeUnit.NANOSECONDS);
                        processResponse(Type.PUT, response, metaData);
                        System.out.println("PUT {key:" + content.getKey() + "} tried to poll from queue. Null?: "
                                + (response == null) + " numResponsesGot:" + numResponsesGot
                                + " parallelResponseToWait: " + numNodesPendingResponse
                                + "; preferred-1: " + (Constants.preferredWrites - 1) + "; preferredOK: "
                                + preferredSatisfied + " quorumOK: " + quorumSatisfied
                                + "; zoneOK: " + zonesSatisfied);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }

            // clean leftovers
            // a) The main thread did a processResponse, due to which the
            // criteria (quorum) was satisfied
            // b) After this, the main thread cuts off adding responses to the
            // queue by the async callbacks

            // An async callback can be invoked between a and b (this is the
            // leftover)
            while(!responseQueue.isEmpty()) {
                Response<ByteArray, Object> response;
                try {
                    response = responseQueuePoll(responseQueue,0,
                                    TimeUnit.NANOSECONDS);
                    processResponse(Type.PUT, response, metaData);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }

            }

            quorumSatisfied = quorum.isQuorumSatisfied(message);
            zonesSatisfied = quorum.isZonesSatisfied(message);

            if(quorumSatisfied && zonesSatisfied) {
                System.out.println("PUT {key:" + content.getKey() + "} succeeded at parallel put stage");

                return;
            } else {
                if(!quorumSatisfied) {
                    System.out.println("PUT {key:" + content.getKey()
                                + "} failed due to insufficient nodes. required=" + Constants.requiredWrites
                                + " success=" + numResponsesGot);
                } else if(!zonesSatisfied) {
                    System.out.println("PUT {key:" + content.getKey()
                                + "} failed due to insufficient zones. required="
                                + Constants.requiredZones + 1 + " success="
                                + metaData.getZoneResponses().size());
                    }
                return;

            }
        } catch(NoSuchElementException e) {
                System.out.println("Response Queue is empty. There may be a bug in PerformParallelPutRequest");
                System.out.println(e);
        } finally {
            System.out.println("PUT {key:" + content.getKey() + "} marking parallel put stage finished");
        }
    }

    private synchronized Response<ByteArray, Object> responseQueuePoll(Queue<Response<ByteArray, Object>> responseQueue, long timeout,
                                                                      TimeUnit timeUnit)
            throws InterruptedException {
        long timeoutMs = timeUnit.toMillis(timeout);
        long timeoutWallClockMs = System.currentTimeMillis() + timeoutMs;
        while(responseQueue.isEmpty() && System.currentTimeMillis() < timeoutWallClockMs) {
            long remainingMs = Math.max(0, timeoutWallClockMs - System.currentTimeMillis());

            this.wait(remainingMs);

        }
        return responseQueue.poll();
    }

    @Override
    public boolean apply(Message<K,V> metaData) {
        /*
        System.out.println(pipeline.getOperation().getSimpleName() + " versioning data - was: "
                    + versioned.getVersion());

        // Okay looks like it worked, increment the version for the caller
        VectorClock versionedClock = (VectorClock) versioned.getVersion();
        versionedClock.incrementVersion(pipelineData.getMaster().getId(), time.getMilliseconds());

        if(logger.isTraceEnabled())
            logger.trace(pipeline.getOperation().getSimpleName() + " versioned data - now: "
                    + versioned.getVersion());*/
        return true;
    }

    public void delete(List<Node> nodes, Content<K,V> content, MetaData metaData){

    }

    private boolean processResponse(Type type, Response<ByteArray, Object> response, MetaData metaData) {
        if (type == Type.PUT) {
            if (response == null) {
                System.out.println("RoutingTimedout on waiting for async ops; parallelResponseToWait: "
                        + numNodesPendingResponse + "; preferred-1: " + (Constants.preferredWrites - 1)
                        + "; quorumOK: " + quorumSatisfied + "; zoneOK: " + zonesSatisfied);
            } else {
                numNodesPendingResponse = numNodesPendingResponse - 1;
                numResponsesGot = numResponsesGot + 1;
                if (response.getValue() instanceof Exception
                        && !(response.getValue() instanceof ObsoleteVersionException)) {
                    System.out.println("PUT handling async put error");


                    if (response.getValue() instanceof QuotaExceededException) {
                        /**
                         * We want to slop ParallelPuts which fail due to
                         * QuotaExceededException.
                         *
                         * TODO Though this is not the right way of doing things, in
                         * order to avoid inconsistencies and data loss, we chose to
                         * slop the quota failed parallel puts.
                         *
                         * As a long term solution - 1) either Quota management
                         * should be hidden completely in a routing layer like
                         * Coordinator or 2) the Server should be able to
                         * distinguish between serial and parallel puts and should
                         * only quota for serial puts
                         *
                         */
                        //pipelineData.getSynchronizer().tryDelegateSlop(response.getNode());
                    }

                    System.out.println("PUT {key} handled async put error");


                } else {
                    metaData.getZoneResponses().add(response.getNode().getZoneId());
                    metaData.incrementPutSuccesses();
                    VoldemortClientShell.factory.getFailureDetector().recordSuccess(response.getNode(), response.getRequestTime());
                }
            }
            return false;
        }
        return true;
    }
}
