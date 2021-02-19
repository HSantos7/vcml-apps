package edu.msu.cse.cops.server;

import edu.msu.cse.cops.metadata.*;
import edu.msu.cse.cops.server.consistency.Configurations;
import edu.msu.cse.cops.server.consistency.Framework;
import edu.msu.cse.cops.server.consistency.cluster.Node;
import edu.msu.cse.cops.server.consistency.types.Content;
import edu.msu.cse.cops.server.consistency.types.Message;
import edu.msu.cse.cops.server.consistency.types.MetaData;
import edu.msu.cse.cops.server.consistency.versioning.Version;
import edu.msu.cse.dkvf.ClientMessageAgent;
import edu.msu.cse.dkvf.DKVFServer;
import edu.msu.cse.dkvf.config.ConfigReader;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class COPSServer<K,V> extends DKVFServer {

	int dcId;// datacenter id
	int pId; // partition id
	int numOfDatacenters;
	int numOfPartitions;

	//Long clock = 0L; // higher bits are Lamport clocks

	Framework<K,V> framework;
	public COPSServer(ConfigReader cnfReader) {
		super(cnfReader);

		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();

		dcId = new Integer(protocolProperties.get("dc_id").get(0));
		pId = new Integer(protocolProperties.get("p_id").get(0));

		numOfDatacenters = new Integer(protocolProperties.get("num_of_datacenters").get(0));
		numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));

		List<Integer> partitions = new ArrayList<>();
		partitions.add(pId);
		Node owner = new Node(dcId + "_" + pId, dcId, partitions);
		framework = new Framework<>(owner);

	}

	public void handleClientMessage(ClientMessageAgent cma) {
		if (cma.getClientMessage().hasGetMessage()) {
			Content<K,V> content = new Content<>((K) cma.getClientMessage().getGetMessage().getKey());
			MetaData metaData = new MetaData();
			metaData.setClientMessageAgent(cma);
			framework.newMessage(new Message<>(Message.Type.GET, content, metaData));
		} else if (cma.getClientMessage().hasPutMessage()) {
			Content<K,V> content = new Content<>((K) cma.getClientMessage().getPutMessage().getKey(),
												 (V) cma.getClientMessage().getPutMessage().getValue());
			MetaData metaData = new MetaData();
			HashMap<String, Version> dependencies = new HashMap<>();
			for (Dependency dep : cma.getClientMessage().getPutMessage().getNearestList()){
				Version version = Configurations.getVersionObject();
				for (NodeVersion nodeVersion : dep.getNodeVersionList()){
					version.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
				}
				dependencies.put(dep.getKey(), version);
			}
			metaData.setDependencies(dependencies);
			metaData.setClientMessageAgent(cma);
			framework.newMessage(new Message<>(Message.Type.PUT, content, metaData));
		}
	}

	@Override
	public void handleServerMessage(ServerMessage sm) {
		if (sm.hasReplicateMessage()) {
			ReplicateMessage rm = sm.getReplicateMessage();
			Content<K,V> content = new Content<>((K) rm.getKey(), (V) rm.getRec().getValue());
			MetaData metaData = new MetaData();
			Version version = Configurations.getVersionObject();
			for (NodeVersion nodeVersion : rm.getRec().getNodeVersionList()){
				version.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
			}
			metaData.setVersion(version);
			HashMap<String, Version> dependencies = new HashMap<>();
			for (Dependency dep : rm.getNearestList()){
				Version versionDep = Configurations.getVersionObject();
				for (NodeVersion nodeVersion : dep.getNodeVersionList()){
					versionDep.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
				}
				dependencies.put(dep.getKey(), versionDep);
			}
			metaData.setDependencies(dependencies);
			framework.replicateMessage(new Message<>(Message.Type.REPLICATE, content, metaData));
		} else if (sm.hasDepCheckMessage()) {
			DependencyCheckMessage cdm = sm.getDepCheckMessage();
			Content<K,V> content = new Content<>((K) cdm.getForKey());
			MetaData metaData = new MetaData();
			HashMap<String, Version> dependencies = new HashMap<>();
			Version version = Configurations.getVersionObject();
			for (NodeVersion nodeVersion : cdm.getDep().getNodeVersionList()){
				version.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
			}
			dependencies.put(cdm.getDep().getKey(), version);
			metaData.setDependencies(dependencies);
			metaData.setNodeToSendResponse(new Node(MainClass.gServer.getDcId() + "_" + cdm.getPId(),"",-1,-1,-1, null));
			framework.newMessage(new Message<>(Message.Type.DEPENDENCY_REQUEST, content, metaData));
		} else if (sm.hasDepResponseMessage()) {
			DependencyResponseMessage drm = sm.getDepResponseMessage();
			Content<K,V> content = new Content<>((K) drm.getForKey());
			MetaData metaData = new MetaData();
			HashMap<String, Version> dependencies = new HashMap<>();
			Version version = Configurations.getVersionObject();
			for (NodeVersion nodeVersion : drm.getDep().getNodeVersionList()){
				version.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
			}
			dependencies.put(drm.getDep().getKey(), version);
			metaData.setDependencies(dependencies);
			framework.newMessage(new Message<>(Message.Type.DEPENDENCY_RESPONSE, content, metaData));
		}
	}

	public void sendDepCheckMessages(String key, List<Dependency> nearestList) {
		for (Dependency dep : nearestList) {
			DependencyCheckMessage dcm = DependencyCheckMessage.newBuilder().setForKey(key).setDep(dep).setPId(pId).build();
			ServerMessage sm = ServerMessage.newBuilder().setDepCheckMessage(dcm).build();
			int partition;
			try {
				partition = findPartition(key);
			} catch (NoSuchAlgorithmException e) {
				protocolLOGGER.severe("Problem finding partition for key " + key);
				return;
			}
			if (partition != pId) {
				String serverId = dcId + "_" + partition;
				sendToServerViaChannel(serverId, sm);
			}
		}

	}

	private int findPartition(String key) throws NoSuchAlgorithmException {
		long hash = edu.msu.cse.dkvf.Utils.getMd5HashLong(key);
		return (int) (hash % numOfPartitions);
	}

	public int getDcId() {
		return dcId;
	}

	public int getNumOfDatacenters() {
		return numOfDatacenters;
	}

	public int getNumOfPartitions() {
		return numOfPartitions;
	}

	public int getpId() {
		return pId;
	}
}
