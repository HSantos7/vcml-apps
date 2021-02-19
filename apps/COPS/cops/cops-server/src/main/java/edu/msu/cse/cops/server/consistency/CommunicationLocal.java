package edu.msu.cse.cops.server.consistency;

import com.google.protobuf.ByteString;
import edu.msu.cse.cops.metadata.NodeVersion;
import edu.msu.cse.cops.metadata.Record;
import edu.msu.cse.cops.server.MainClass;
import edu.msu.cse.cops.server.consistency.cluster.Node;
import edu.msu.cse.cops.server.consistency.types.Content;
import edu.msu.cse.cops.server.consistency.types.MetaData;
import edu.msu.cse.cops.server.consistency.versioning.Version;
import edu.msu.cse.dkvf.Storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class CommunicationLocal<K,V> implements CommunicationInterface.internal<K,V> {
    GroupMembershipInterface groupMembership;
    public CommunicationLocal(GroupMembershipInterface groupMembership) {
        this.groupMembership = groupMembership;
    }
    //private interface
    @Override
    public void get(Node node, Content<K, V> content, MetaData metaData, Callback callback) {
        List<Record> result = new ArrayList<>();
        Storage.StorageStatus ss = MainClass.gServer.read((String) content.getKey(),
                                                         (Record rec) -> {
                                                                return true;
                                                                },
                                                          result);

        if (ss == Storage.StorageStatus.SUCCESS) {
            content.setValue((V) result.get(0).getValue());
            Version version = Configurations.getVersionObject();
            for (NodeVersion nodeVersion : result.get(0).getNodeVersionList()){
                version.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
            }
            metaData.setVersion(version);
            metaData.incrementGetSuccesses();
        } else {
            metaData.setVersion(null);
        }
    }

    @Override
    public void delete(Node node, Content<K, V> content, MetaData metaData, Callback callback) {
    }


    @Override
    public void put(Node node, Content<K, V> content, MetaData metaData) {
        List<NodeVersion> nodeVersions = new ArrayList<>();
        for (Map.Entry<String, Long> entry : metaData.getVersion().getVersions().entrySet()){
            nodeVersions.add(NodeVersion.newBuilder().setNode(entry.getKey()).setVersion(entry.getValue()).build());
        }
        Record rec = Record.newBuilder().setValue((ByteString) content.getValue()).addAllNodeVersion(nodeVersions).build();
        Storage.StorageStatus ss = MainClass.gServer.insert((String) content.getKey(), rec);
        if (ss == Storage.StorageStatus.SUCCESS){
            metaData.incrementPutSuccesses();
        }
    }

    @Override
    public Version getActualVersion(Content<K, V> content, MetaData metaData) {
        List<Record> result = new ArrayList<>();
        Storage.StorageStatus ss = MainClass.gServer.read((String) content.getKey(), (Record rec) -> {
            return true;
        }, result);

        if (ss == Storage.StorageStatus.SUCCESS) {
            Version version = Configurations.getVersionObject();
            for (NodeVersion nodeVersion : result.get(0).getNodeVersionList()){
                version.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
            }
            return version;
        } else {
            return null;
        }
    }






}
