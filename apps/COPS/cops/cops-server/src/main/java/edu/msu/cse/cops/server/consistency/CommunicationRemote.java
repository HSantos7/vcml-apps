package edu.msu.cse.cops.server.consistency;

import com.google.protobuf.ByteString;
import edu.msu.cse.cops.metadata.*;
import edu.msu.cse.cops.server.MainClass;
import edu.msu.cse.cops.server.consistency.cluster.Node;
import edu.msu.cse.cops.server.consistency.types.Content;
import edu.msu.cse.cops.server.consistency.types.Message;
import edu.msu.cse.cops.server.consistency.types.MetaData;
import edu.msu.cse.cops.server.consistency.versioning.Version;
import edu.msu.cse.dkvf.ClientMessageAgent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class CommunicationRemote<K,V> implements CommunicationInterface.external<K,V> {
    GroupMembershipInterface groupMembership;
    public CommunicationRemote(GroupMembershipInterface groupMembership) {
        this.groupMembership = groupMembership;
    }

    //public interface
    @Override
    public void sendGetResponse(Content<K, V> content, MetaData metaData, Callback callback) {
        ClientReply cr;
        if (metaData.getVersion() != null && metaData.getGetSuccesses() >= Configurations.requiredReads) {
            List<NodeVersion> nodeVersions = new ArrayList<>();
            for (Map.Entry<String, Long> entry :metaData.getVersion().getVersions().entrySet()){
                nodeVersions.add(NodeVersion.newBuilder().setNode(entry.getKey()).setVersion(entry.getValue()).build());
            }
            Record rec = Record.newBuilder().setValue((ByteString) content.getValue()).addAllNodeVersion(nodeVersions).build();
            cr = ClientReply.newBuilder().setStatus(true).setGetReply(GetReply.newBuilder().setRecord(rec)).build();
        } else {
            cr = ClientReply.newBuilder().setStatus(false).build();
        }
        ((ClientMessageAgent) metaData.getClientMessageAgent()).sendReply(cr);
    }

    @Override
    public void sendPutResponse(Content<K, V> content, MetaData metaData, Callback callback) {
        ClientReply cr;
        if (metaData.getPutSuccesses() >= Configurations.requiredWrites) {
            List<NodeVersion> nodeVersions = new ArrayList<>();
            for (Map.Entry<String, Long> entry : metaData.getVersion().getVersions().entrySet()){
                nodeVersions.add(NodeVersion.newBuilder().setNode(entry.getKey()).setVersion(entry.getValue()).build());
            }
            cr = ClientReply.newBuilder().setStatus(true).setPutReply(PutReply.newBuilder().addAllVersion(nodeVersions)).build();
        } else {
            cr = ClientReply.newBuilder().setStatus(false).build();
        }
        ((ClientMessageAgent) metaData.getClientMessageAgent()).sendReply(cr);
    }

    @Override
    public void sendDeleteResponse(Content<K, V> content, MetaData metaData, Callback callback) {

    }

    @Override
    public void replicate(Node node, Content<K, V> content, MetaData metaData, Callback callback) {
        List<NodeVersion> nodeVersions = new ArrayList<>();
        for (Map.Entry<String, Long> entry : metaData.getVersion().getVersions().entrySet()){
            nodeVersions.add(NodeVersion.newBuilder().setNode(entry.getKey()).setVersion(entry.getValue()).build());
        }
        Record rec = Record.newBuilder().setValue((ByteString) content.getValue()).addAllNodeVersion(nodeVersions).build();
        List<Dependency> nearestList = new ArrayList<>();
        for (Map.Entry<String, Version> dependency : metaData.getDependencies().entrySet()){
            List<NodeVersion> nodeVersionsDep = new ArrayList<>();
            for (Map.Entry<String, Long> entry : dependency.getValue().getVersions().entrySet()){
                nodeVersionsDep.add(NodeVersion.newBuilder().setNode(entry.getKey()).setVersion(entry.getValue()).build());
            }
            nearestList.add(Dependency.newBuilder().setKey(dependency.getKey()).addAllNodeVersion(nodeVersionsDep).build());
        }
        ServerMessage sm = ServerMessage.newBuilder().setReplicateMessage(ReplicateMessage.newBuilder().setKey((String) content.getKey())
                                                                                                       .setRec(rec)
                                                                                                       .addAllNearest(nearestList)).build();
        MainClass.gServer.sendToServerViaChannel(node.getId(),sm);
        metaData.incrementReplicateSuccesses();
    }

    @Override
    public void sendDependenciesCheck(Content<K, V> content, MetaData metaData){
        List<Dependency> remainingDeps = new ArrayList<>();
        for (Map.Entry<String, Version> dependency : metaData.getRemainingDependencies().entrySet()){
            List<NodeVersion> nodeVersionsDep = new ArrayList<>();
            for (Map.Entry<String, Long> entry : dependency.getValue().getVersions().entrySet()){
                nodeVersionsDep.add(NodeVersion.newBuilder().setNode(entry.getKey()).setVersion(entry.getValue()).build());
            }
            remainingDeps.add(Dependency.newBuilder().setKey(dependency.getKey()).addAllNodeVersion(nodeVersionsDep).build());
        }
        MainClass.gServer.sendDepCheckMessages((String) content.getKey(), remainingDeps);
    }

    @Override
    public void sendDependenciesResponse(Message<K, V> message) {
        Content<K,V> content = message.getContent();
        MetaData metaData = message.getMetaData();
        for (Map.Entry<String, Version> dependency : metaData.getDependencies().entrySet()){
            List<NodeVersion> nodeVersionsDep = new ArrayList<>();
            for (Map.Entry<String, Long> entry : dependency.getValue().getVersions().entrySet()){
                nodeVersionsDep.add(NodeVersion.newBuilder().setNode(entry.getKey()).setVersion(entry.getValue()).build());
            }
            Dependency dep = Dependency.newBuilder().setKey(dependency.getKey()).addAllNodeVersion(nodeVersionsDep).build();
            DependencyResponseMessage drm = DependencyResponseMessage.newBuilder().setForKey((String) content.getKey()).setDep(dep).build();
            ServerMessage sm = ServerMessage.newBuilder().setDepResponseMessage(drm).build();
            MainClass.gServer.sendToServerViaChannel(metaData.getNodeToSendResponse().getId(), sm);
            return;
        }
    }
}
