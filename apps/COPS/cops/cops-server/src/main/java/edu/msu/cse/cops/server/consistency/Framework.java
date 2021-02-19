package edu.msu.cse.cops.server.consistency;

import edu.msu.cse.cops.server.consistency.cluster.Node;
import edu.msu.cse.cops.server.consistency.types.Content;
import edu.msu.cse.cops.server.consistency.types.DependencyRequest;
import edu.msu.cse.cops.server.consistency.types.Message;
import edu.msu.cse.cops.server.consistency.types.MetaData;
import edu.msu.cse.cops.server.consistency.utils.SystemTime;
import edu.msu.cse.cops.server.consistency.utils.Time;
import edu.msu.cse.cops.server.consistency.utils.Utils;
import edu.msu.cse.cops.server.consistency.versioning.Occurred;

public class Framework<K,V> implements API<K,V> {
    protected final Time time;
    OrderInterface<K, V> order;
    GroupMembershipInterface groupMembership;
    QuorumInterface<K,V> quorum;
    CommunicationInterface.internal<K,V> communicationLocal;
    CommunicationInterface.external<K,V> communicationRemote;
    ReplicateInterface<K,V> replicate;
    DeliveryConditionInterface<K,V> deliveryCondition;

    public Framework(Node node) {
        this.time = Utils.notNull(SystemTime.INSTANCE);
        this.groupMembership = Configurations.getGroupMembershipClass(node);
        this.communicationLocal = Configurations.getCommunicationLocalClass(groupMembership);
        this.communicationRemote = Configurations.getCommunicationRemoteClass(groupMembership);
        this.order = Configurations.getOrderClass(Configurations.maxMetadataRefreshAttempts, groupMembership, communicationLocal);
        this.quorum = Configurations.getQuorumClass();
        this.deliveryCondition = Configurations.getDeliveryConditionClass(order, communicationLocal, communicationRemote, groupMembership);
        this.replicate = Configurations.getReplicateClass(groupMembership, quorum, communicationLocal, communicationRemote, deliveryCondition);
    }

    @Override
    public void newMessage(Message<K, V> incomingMessage) {
        long startTime = time.getNanoseconds();
        Content<K,V> content = incomingMessage.getContent();
        MetaData metadata = incomingMessage.getMetaData();
        switch (incomingMessage.getType()){
            case PUT:{
                if(isTimestamper()){
                    orderMessage(content, metadata, startTime);
                    replicate.apply(incomingMessage);
                    communicationRemote.sendPutResponse(content, metadata,null);
                    replicate.replicate(incomingMessage);
                }else{
                    int timestamper = groupMembership.getTimestamper();
                    //TODO o que fazer em caso de nao ser timestamper
                }
                break;
            }
            case GET:{
                communicationLocal.get(null, content, metadata,null);
                communicationRemote.sendGetResponse(content, metadata,null);
                break;
            }
            case DELETE:{
                break;
            }
            case DEPENDENCY_REQUEST:{
                communicationLocal.get(null, content, metadata,null);
                if (metadata.getVersion() != null) {
                    if(order.compareMessages(metadata.getVersion(), metadata.getDependencies().entrySet().iterator().next().getValue()) != Occurred.AFTER){
                        communicationRemote.sendDependenciesResponse(incomingMessage);
                    }
                }
                deliveryCondition.addToRemoteWaitingDep(new DependencyRequest<>(content.getKey(),
                                                                                metadata.getDependencies().entrySet().iterator().next().getValue(),
                                                                                metadata.getNodeToSendResponse()));
                break;
            }
            case DEPENDENCY_RESPONSE:{
                if(deliveryCondition.removeRemoteWaitingDep(incomingMessage))
                    replicate.apply(incomingMessage);
                break;
            }
        }
    }

    @Override
    public void replicateMessage(Message<K, V> replicateMessage) {
        //System.out.println("Replicate incoming: " + replicateMessage.getContent().getKey() + " " + replicateMessage.getContent().getValue());
        //System.out.println("Replication result: " + replicate.apply(replicateMessage));
        //System.out.println("Local clock after Replication message: " + order.getClock().getVersions());

        replicate.apply(replicateMessage);
    }

    @Override
    public void getReplicateState() { }

    private boolean isTimestamper(){
        //RETORNA SEMPRE TRUE
        return groupMembership.getRole();
    }

    private void orderMessage(Content<K,V> content, MetaData metadata, long startTime){
        metadata.setVersion(order.timeStamping(content, startTime));
    }

}
