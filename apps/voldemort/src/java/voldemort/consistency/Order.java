package voldemort.consistency;

import voldemort.VoldemortClientShell;
import voldemort.cluster.failuredetector.FailureDetector;
import voldemort.consistency.cluster.Node;
import voldemort.consistency.types.Content;
import voldemort.consistency.types.MetaData;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.utils.serialization.StringSerializer;
import voldemort.consistency.versioning.Occurred;
import voldemort.consistency.versioning.Version;
import voldemort.consistency.versioning.Versioned;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.utils.Utils;

import java.util.List;
import java.util.Map;

public class Order<K,V> implements OrderInterface<K,V>{

    GroupMembershipInterface groupMembership;
    protected final Time time;

    protected int metadataRefreshAttempts;
    FailureDetector failureDetector;
    CommunicationInterface.internal<K,V> communicationLocal;
    public Order(int maxMetadataRefreshAttempts, GroupMembershipInterface groupMembership, CommunicationInterface.internal<K,V> communicationLocal) {
        this.time = Utils.notNull(SystemTime.INSTANCE);
        this.metadataRefreshAttempts = maxMetadataRefreshAttempts;
        this.groupMembership = groupMembership;
        this.failureDetector = VoldemortClientShell.factory.getFailureDetector();
        this.communicationLocal = communicationLocal;
    }

    @Override
    public MetaData timeStamping(Content<K, V> content, long startTime) {
        MetaData metadata = new MetaData(communicationLocal.getActualVersion(content, null));
        metadata.setStartTimeNs(startTime);
        int currentNode = 0;
        int nodeIndex = 0;
        metadata.clearZoneResponses();
        List<Node> nodes = groupMembership.getNodes(new ByteArray(VoldemortClientShell.serializeKey(content.getKey())),
                Constants.requiredWrites); //TODO: Adaptar para K

        long startMasterMs = -1;
        long startMasterNs = -1;

        Node node = null;
        for(; currentNode < nodes.size(); currentNode++) {
            node = nodes.get(currentNode);
            nodeIndex++;
            Version versionedClock = null;
            try {
                versionedClock = (Version) Constants.getVersionType().newInstance();
                versionedClock = metadata.getVersion();
                versionedClock = versionedClock.incremented(node.getId(), time.getMilliseconds());
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
            byte[] aux = null;
            if(content.getValue() instanceof String){
                aux = new StringSerializer().toBytes((String)content.getValue());
            }
            final Versioned<byte[]> versionedCopy = new Versioned<byte[]>(aux,
                    versionedClock);

            System.out.println("Attempt #" + (currentNode + 1) + " to perform put (node "
                    + node.getId() + ")");
            metadata.setVersioned(versionedCopy);

            long start = System.nanoTime();
            try {
                communicationLocal.put(node, content, metadata, null);
                long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;
                metadata.incrementPutSuccesses();
                failureDetector.recordSuccess(node, requestTime);

                System.out.println("Put on node " + node.getId() + " succeeded, using as master");

                metadata.setMaster(node);
                metadata.addZoneResponses(node.getZoneId());
                currentNode++;
                break;
            } catch(Exception e) {
                long requestTime = (System.nanoTime() - start) / Time.NS_PER_MS;

                System.out.println("Master PUT at node " + currentNode + "(" + node.getHost() + ")"
                        + " failed (" + e.getMessage() + ") in "
                        + (System.nanoTime() - start) + " ns" + " (keyRef: "
                        + System.identityHashCode(content.getKey()) + ")");
                System.exit(-1);
            }
        }

        System.out.println("PUT {key:" + content.getKey() + "} currentNode=" + currentNode + " nodes.size()="
                + nodes.size());

        if(metadata.getPutSuccesses() < 1) {
            System.out.println("No master node succeeded!");
            return null;
        }

        // There aren't any more requests to make...
        if(currentNode == nodes.size()) {
            if(metadata.getPutSuccesses() < Constants.preferredWrites) {
                System.out.println("ERROR");
                return null;
            } else {
                if(Constants.requiredZones != 0) {

                    int zonesSatisfied = metadata.getZoneResponses().size();
                    if(zonesSatisfied >= (Constants.requiredZones + 1)) {
                        return null;
                    } else {
                        System.out.println("ERROR");
                        return null;
                    }

                } else {
                    System.out.println("Finished master PUT for key "
                            + content.getKey() + " (keyRef: "
                            + System.identityHashCode(content.getKey()) + "); started at "
                            + startMasterMs + " took "
                            + (System.nanoTime() - startMasterNs) + " ns on node "
                            + (node == null ? "NULL" : node.getId()) + "("
                            + (node == null ? "NULL" : node.getHost()) + "); now complete");
                    return metadata;
                }
            }
        } else {
            System.out.println("Finished master PUT for key " + content.getKey()
                    + " (keyRef: " + System.identityHashCode(content.getKey()) + "); started at "
                    + startMasterMs + " took " + (System.nanoTime() - startMasterNs)
                    + " ns on node " + (node == null ? "NULL" : node.getId()) + "("
                    + (node == null ? "NULL" : node.getHost()) + ")");
            return metadata;
        }
    }

    @Override
    public Occurred compareMessages(Version ver1, Version ver2) {
        return null;
    }

    @Override
    public void updateClock() {

    }

    @Override
    public void updateClock(Map<String, Long> newVersion) {

    }
}
