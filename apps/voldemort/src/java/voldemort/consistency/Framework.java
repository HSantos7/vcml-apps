package voldemort.consistency;

import voldemort.VoldemortClientShell;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.consistency.cluster.Node;
import voldemort.consistency.types.Message;
import voldemort.consistency.types.MetaData;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.utils.pipeline.Response;
import voldemort.consistency.utils.serialization.StringSerializer;
import voldemort.consistency.versioning.Versioned;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Framework<K,V> implements API<K,V> {
    protected final Time time;
    OrderInterface<K, V> order;
    GroupMembershipInterface groupMembership;
    QuorumInterface<K,V> quorum;
    CommunicationInterface.internal<K,V> communicationLocal;
    CommunicationInterface.external<K,V> communicationRemote;
    ReplicateInterface<K,V> replicate;
    DeliveryConditionInterface<K,V> deliveryCondition;
    FailureDetector failureDetector;

    public Framework() {
        this.time = Utils.notNull(SystemTime.INSTANCE);
        this.groupMembership = Constants.getGroupMembershipClass(null);
        this.communicationLocal = Constants.getCommunicationLocalClass(groupMembership);
        this.communicationRemote = Constants.getCommunicationRemoteClass(groupMembership);
        this.order = Constants.getOrderClass(Constants.maxMetadataRefreshAttempts, groupMembership, communicationLocal);
        this.quorum = Constants.getQuorumClass();
        this.deliveryCondition = Constants.getDeliveryConditionClass(order, communicationLocal, communicationRemote, groupMembership);
        this.replicate = Constants.getReplicateClass(groupMembership, quorum, communicationLocal, communicationRemote, deliveryCondition);
        this.failureDetector = VoldemortClientShell.factory.getFailureDetector();
    }

    @Override
    public void newMessage(Message<K,V> message){
        long startTime = time.getNanoseconds();
        switch (message.getType()){
            case PUT:{
                if(isTimestamper()){
                    orderMessage(message, startTime);
                    replicate.replicate(message);
                }else{
                    int timestamper = groupMembership.getTimestamper();
                    //TODO o que fazer em caso de nao ser timestamper
                }

            }
            case GET:{
                List<Node> nodes = groupMembership.getNodes(new ByteArray(VoldemortClientShell.serializeKey(message.getContent().getKey())),
                        Constants.requiredReads);
                int attempts = Math.min(Constants.preferedReads, nodes.size());
                final Map<Integer, Response<ByteArray, Object>> responses = new ConcurrentHashMap<>();
                final CountDownLatch latch = new CountDownLatch(attempts);

                System.out.println("Attempting " + attempts + " " + "Get"
                        + " operations in parallel for key " + message.getContent().getKey());

                final AtomicBoolean isResponseProcessed = new AtomicBoolean(false);

                for (int i = 0; i < attempts; i++) {
                    final Node node = nodes.get(i);

                    final long startMs = System.currentTimeMillis();

                    Callback callback = (result, requestTime) -> {
                        System.out.println("Get"
                                + " response received (" + requestTime + " ms.) from node "
                                + node.getId() + "for key " + message.getContent().getKey());

                        Response<ByteArray, Object> response = new Response<>(node,
                                new ByteArray(VoldemortClientShell.serializeKey(message.getContent().getKey())),
                                result,
                                requestTime);
                        System.out.println("Finished Get"
                                + " for key " + message.getContent().getKey()
                                + " (keyRef: " + System.identityHashCode(message.getContent().getKey())
                                + "); started at " + startMs + " took " + requestTime
                                + " ms on node " + node.getId() + "(" + node.getHost() + ")");

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
                        if (isResponseProcessed.get() && response.getValue() instanceof Exception) {
                            System.out.println("ERROR");
                        }
                    };

                    System.out.println("Submitting Get"
                            + " request on node " + node.getId() + " for key " + message.getContent().getKey());
                    communicationLocal.get(node, message.getContent(), message.getMetaData(), callback);
                }

                try {
                    latch.await(Constants.getOpTimeoutInMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    System.out.println("ERROR");
                }

                for (Response<ByteArray, Object> response: responses.values()) {
                    if (response.getValue() instanceof Exception) {
                        System.out.println("ERROR");
                        return;
                    } else {
                        message.getMetaData().incrementGetSuccesses();
                        Response<ByteArray, V> rCast = Utils.uncheckedCast(response);
                        message.getMetaData().getResponses().add((Response<ByteArray, Object>) rCast);
                        failureDetector.recordSuccess(response.getNode(), response.getRequestTime());
                        message.getMetaData().getZoneResponses().add(response.getNode().getZoneId());
                    }
                }
                isResponseProcessed.set(true);

                System.out.println("GET for key " + message.getContent().getKey() + " (keyRef: "
                        + System.identityHashCode(message.getContent().getKey()) + "); successes: "
                        + message.getMetaData().getGetSuccesses() + " preferred: " + Constants.preferedReads + " required: "
                        + Constants.requiredReads);

                if (message.getMetaData().getGetSuccesses() < Constants.requiredReads) {
                    //ADD INSUFFICIENT SUCCESSES EVENT
                    System.out.println("Insufficient Successes");

                } else {

                    if (Constants.requiredZones != 0) {

                        int zonesSatisfied = message.getMetaData().getZoneResponses().size();
                        if (zonesSatisfied >= (Constants.requiredZones + 1)) {
                            //ADD INSUFFICIENT ZONES EVENT
                            return;
                        } else {
                            System.out.println("Operation Get"
                                    + "failed due to insufficient zone responses, required "
                                    + Constants.requiredZones + " obtained "
                                    + zonesSatisfied + " " + message.getMetaData().getZoneResponses()
                                    + " for key " + message.getContent().getKey());
                        }

                    }
                }
                List<Versioned<byte[]>> results = new ArrayList<>();

                for(Response<?, ?> response: message.getMetaData().getResponses()) {
                    List<Versioned<byte[]>> value = (List<Versioned<byte[]>>) response.getValue();

                    if(value != null)
                        results.addAll(value);
                }
                if(results.size() == 0){
                    if (Constants.getValueType().getSimpleName().equals("String") ) {
                        message.getMetaData().setVersioned(null);
                    }
                } else if(results.size() == 1){
                    if (Constants.getValueType().getSimpleName().equals("String") ){
                        StringSerializer StringSer = new StringSerializer();
                        Versioned<String> result = new Versioned<>(StringSer.toObject(results.get(0).getValue()), results.get(0).getVersion());
                        message.getMetaData().setVersioned(result);
                    }
                }
                else
                    System.out.println("Unresolved versions returned from get(" + message.getContent().getKey()
                            + ") = " + results);
                break;
            }
            case DELETE:{
                message.getMetaData().setVersion(communicationLocal.getActualVersion(message.getContent(), message.getMetaData()));
                if(message.getMetaData().getVersion() == null)
                    return;
                List<Node> nodes = groupMembership.getNodes(new ByteArray(VoldemortClientShell.serializeKey(message.getContent().getKey())),
                        Constants.requiredWrites);
                AtomicBoolean isOperationCompleted = new AtomicBoolean(false);
                final Map<Integer, Response<ByteArray, Object>> responses = new ConcurrentHashMap<>();
                int attempts = nodes.size();
                int blocks = Math.min(Constants.preferredWrites, attempts);
                final CountDownLatch attemptsLatch = new CountDownLatch(attempts);
                final CountDownLatch blocksLatch = new CountDownLatch(blocks);

                System.out.println("Attempting " + attempts + " Delete" + " operations in parallel");

                long beginTime = System.nanoTime();

                for(int i = 0; i < attempts; i++) {
                    final Node node = nodes.get(i);

                    Callback callback = (result, requestTime) -> {
                        System.out.println("Delete response received (" + requestTime + " ms.) from node "
                                + node.getId());

                        Response<ByteArray, Object> response = new Response<ByteArray, Object>(node,
                                new ByteArray(VoldemortClientShell.serializeKey(message.getContent().getKey())),
                                result,
                                requestTime);

                        System.out.println(attemptsLatch.getCount() + " attempts remaining. Will block "
                                + " for " + blocksLatch.getCount() + " more ");

                        responses.put(node.getId(), response);

                        if(response.getValue() instanceof Exception && isOperationCompleted.get()) {
                            System.out.println("ERROR");
                        }

                        attemptsLatch.countDown();
                        blocksLatch.countDown();

                    };

                    System.out.println("Submitting delete request on node " + node.getId());

                    communicationLocal.delete(node, message.getContent(), message.getMetaData(), callback);
                }

                try {
                    long ellapsedNs = System.nanoTime() - beginTime;
                    long remainingNs = (Constants.deleteOpTimeoutInMs * Time.NS_PER_MS) - ellapsedNs;
                    if(remainingNs > 0) {
                        blocksLatch.await(remainingNs, TimeUnit.NANOSECONDS);
                    }
                } catch(InterruptedException e) {
                    System.out.println("ERROR");
                }

                if(processDeleteResponses(responses, message.getMetaData()) == 0){
                    isOperationCompleted.set(true);
                    return;
                }

                // wait for more responses in case we did not have enough successful
                // response to achieve the required count
                boolean quorumSatisfied = true;
                if(message.getMetaData().getGetSuccesses() < Constants.requiredWrites) {
                    long ellapsedNs = System.nanoTime() - beginTime;
                    long remainingNs = (Constants.deleteOpTimeoutInMs * Time.NS_PER_MS) - ellapsedNs;
                    if(remainingNs > 0) {
                        try {
                            attemptsLatch.await(remainingNs, TimeUnit.NANOSECONDS);
                        } catch(InterruptedException e) {

                        }

                        if(processDeleteResponses(responses, message.getMetaData()) == 0){
                            isOperationCompleted.set(true);
                            return;
                        }
                    }

                    if(message.getMetaData().getGetSuccesses() < Constants.requiredWrites) {
                        quorumSatisfied = false;
                    }
                }

                if(quorumSatisfied) {
                    if(Constants.requiredZones != 0) {
                        int zonesSatisfied = message.getMetaData().getZoneResponses().size();
                        if(zonesSatisfied >= (Constants.requiredZones + 1)) {
                            return;
                        } else {
                            long timeMs = (System.nanoTime() - beginTime) / Time.NS_PER_MS;

                            if((Constants.deleteOpTimeoutInMs - timeMs) > 0) {
                                try {
                                    attemptsLatch.await(Constants.deleteOpTimeoutInMs - timeMs, TimeUnit.MILLISECONDS);
                                } catch(InterruptedException e) {

                                }

                                if(processDeleteResponses(responses, message.getMetaData()) == 0){
                                    isOperationCompleted.set(true);
                                    return;
                                }
                            }

                            if(message.getMetaData().getZoneResponses().size() >= (Constants.requiredZones + 1)) {
                                return;
                            } else {
                                System.out.println("ERROR");
                            }
                        }
                    } else {
                        {
                            isOperationCompleted.set(true);
                            return;
                        }
                    }
                }
                break;
            }
            case DEPENDENCY_REQUEST:
            case DEPENDENCY_RESPONSE:
                break;
        }
    }

    @Override
    public void replicateMessage(Message<K, V> replicateMessage) {

    }

    @Override
    public void getReplicateState() {

    }

    private boolean isTimestamper(){
        return groupMembership.getRole(0); //TODO: Adaptar para n nodes
    }

    private void orderMessage(Message<K,V> message, long startTime){
        message.setMetaData(order.timeStamping(message.getContent(), startTime));
    }

    private int processDeleteResponses(Map<Integer, Response<ByteArray, Object>> responses, MetaData metaData){
        for(Map.Entry<Integer, Response<ByteArray, Object>> responseEntry: responses.entrySet()) {
            if(responseEntry.getValue().getValue() instanceof Exception) {
                System.out.println("ERROR");
            } else {
                metaData.incrementDeleteSuccesses();
                VoldemortClientShell.factory.getFailureDetector().recordSuccess(responseEntry.getValue().getNode(), responseEntry.getValue().getRequestTime());
                metaData.getZoneResponses().add(responseEntry.getValue().getNode().getZoneId());
                Response<ByteArray, V> rCast = Utils.uncheckedCast(responseEntry.getValue());
                metaData.getResponses().add((Response<ByteArray, Object>) rCast);
                responses.remove(responseEntry.getKey());
            }
        }
        return metaData.getDeleteSuccesses();
    }





}
