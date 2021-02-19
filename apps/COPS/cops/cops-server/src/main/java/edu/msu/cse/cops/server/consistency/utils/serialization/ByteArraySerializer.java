package edu.msu.cse.cops.server.consistency.utils.serialization;

import edu.msu.cse.cops.server.consistency.utils.ByteArray;

public final class ByteArraySerializer implements Serializer<ByteArray> {

    public byte[] toBytes(ByteArray object) {
        return object.get();
    }

    public ByteArray toObject(byte[] bytes) {
        return new ByteArray(bytes);
    }
}
