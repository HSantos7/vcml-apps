package edu.msu.cse.dkvf.comparator;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.msu.cse.cops.metadata.NodeVersion;
import edu.msu.cse.cops.metadata.Record;
import edu.msu.cse.cops.server.consistency.Configurations;
import edu.msu.cse.cops.server.consistency.versioning.Occurred;
import edu.msu.cse.cops.server.consistency.versioning.Version;

import java.util.Comparator;

public class RecordCompartor implements Comparator<byte[]>, java.io.Serializable{

    public int compare(byte[] b1, byte[] b2) {

        Record record1;
        Record record2;
        try {
            record1 = Record.parseFrom(b1);
            record2 = Record.parseFrom(b2);
            Version version1 = Configurations.getVersionObject();
            for (NodeVersion nodeVersion : record1.getNodeVersionList()){
                version1.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
            }
            Version version2 = Configurations.getVersionObject();
            for (NodeVersion nodeVersion : record2.getNodeVersionList()){
                version2.updateVersion(nodeVersion.getNode(), nodeVersion.getVersion(), System.currentTimeMillis());
            }
            // we want to put records with higher ut first:
            if (version1.compare(version2) != Occurred.AFTER )
                return -1;
            return 1;
        } catch (InvalidProtocolBufferException e) {
            System.err.println("Invalid byte[] to parse records inside edu.msu.cse.dkvf.comparator.");
            e.printStackTrace();
            return -1;
        }
    }

}
