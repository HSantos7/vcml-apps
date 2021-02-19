package edu.msu.cse.cops.client;

import com.google.protobuf.ByteString;
import edu.msu.cse.cops.metadata.*;
import edu.msu.cse.cops.server.consistency.Configurations;
import edu.msu.cse.cops.server.consistency.versioning.Occurred;
import edu.msu.cse.cops.server.consistency.versioning.Version;
import edu.msu.cse.dkvf.DKVFClient;
import edu.msu.cse.dkvf.ServerConnector.NetworkStatus;
import edu.msu.cse.dkvf.Utils;
import edu.msu.cse.dkvf.config.ConfigReader;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class COPSClient extends DKVFClient{

	int dcId;
	int numOfPartitions;
	
	HashMap<String, Version> nearest;
	
	public COPSClient(ConfigReader cnfReader) {
		super(cnfReader);
		HashMap<String, List<String>> protocolProperties = cnfReader.getProtocolProperties();
		numOfPartitions = new Integer(protocolProperties.get("num_of_partitions").get(0));
		dcId = new Integer(protocolProperties.get("dc_id").get(0));
		nearest = new HashMap<>();
	}

	@Override
	public boolean put(String key, byte[] value) {
		try {
			PutMessage pm = PutMessage.newBuilder().setKey(key).setValue(ByteString.copyFrom(value)).addAllNearest(getDcTimeItems()).build();
			ClientMessage cm = ClientMessage.newBuilder().setPutMessage(pm).build();
			int partition = findPartition(key);
			String serverId = dcId + "_" + partition;
			if (sendToServer(serverId, cm) == NetworkStatus.FAILURE)
				return false;
			ClientReply cr = readFromServer(serverId);

			if (cr != null && cr.getStatus()) {
				nearest.clear();
				Version version = Configurations.getVersionObject();
				for (NodeVersion nodeVersion : cr.getPutReply().getVersionList()){
					version.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
				}
				nearest.put(key, version);
				//System.out.println("SERVER COM A KEY: " + key);
				return true;
			} else {
				//System.out.println("Server could not put the key= " + key);
				protocolLOGGER.severe("Server could not put the key= " + key);
				return false;
			}
		} catch (Exception e) {
			//System.out.println("Failed to put due to exception" + e.getMessage());
			protocolLOGGER.severe(Utils.exceptionLogMessge("Failed to put due to exception", e));
			return false;
		}
	}

	@Override
	public byte[] get(String key) {
		try {
			GetMessage gm = GetMessage.newBuilder().setKey(key).build();
			ClientMessage cm = ClientMessage.newBuilder().setGetMessage(gm).build();
			int partition = findPartition(key);
			String serverId = dcId + "_" + partition;
			if (sendToServer(serverId, cm) == NetworkStatus.FAILURE)
				return null;
			ClientReply cr = readFromServer(serverId);
			if (cr != null && cr.getStatus()) {
				Version version = Configurations.getVersionObject();
				for (NodeVersion nodeVersion : cr.getGetReply().getRecord().getNodeVersionList()){
					version.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
					//System.out.println(cr.getGetReply().getRecord().getValue() + " " + nodeVersion.getVersion());
				}
				updateNearest(key, version);
				return cr.getGetReply().getRecord().getValue().toByteArray();
			} else {
				protocolLOGGER.severe("Server could not get the key= " + key);
				return null;
			}
		} catch (Exception e) {
			protocolLOGGER.severe(Utils.exceptionLogMessge("Failed to get due to exception", e));
			return null;
		}
	}
	
	private int findPartition(String key) throws NoSuchAlgorithmException {
		long hash = Utils.getMd5HashLong(key);
		return (int) (hash % numOfPartitions);
	}
	
	private List<Dependency> getDcTimeItems() {
		List<Dependency> result = new ArrayList<>();
		for (Map.Entry<String, Version> entry : nearest.entrySet()) {
			List<NodeVersion> nodeVersions = new ArrayList<>();
			for (Map.Entry<String, Long> version : entry.getValue().getVersions().entrySet())
				nodeVersions.add(NodeVersion.newBuilder().setNode(version.getKey()).setVersion(version.getValue()).build());
			Dependency dep = Dependency.newBuilder().setKey(entry.getKey()).addAllNodeVersion(nodeVersions).build();
			result.add(dep);
		}
		return result;
	}
	
	private void updateNearest (String key, Version version){
		if (nearest.containsKey(key)){
			if(version.compare(nearest.get(key)) == Occurred.BEFORE) {
				nearest.put(key, nearest.get(key));
			}else {
				nearest.put(key, version);
			}
		} else
			nearest.put(key, version);
	}
}
