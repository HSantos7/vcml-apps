package edu.msu.cse.dkvf.comparator;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.msu.cse.cops.metadata.Record;

import java.util.Comparator;

public class RecordCompartor implements Comparator<byte[]>, java.io.Serializable{

	public int compare(byte[] b1, byte[] b2) {

		Record record1;
		Record record2;
		try {
			record1 = Record.parseFrom(b1);
			record2 = Record.parseFrom(b2);
			// we want to put records with higher ut first:
			if (record1.getVersion() > record2.getVersion())
				return -1;
			return 1;
		} catch (InvalidProtocolBufferException e) {
			System.err.println("Invalid byte[] to parse records inside comparator.");
			e.printStackTrace();
			return -1;
		}
	}

}
