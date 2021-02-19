package voldemort.server.protocol.vold;

import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.utils.ByteUtils;
import voldemort.consistency.versioning.Versioned;
import voldemort.store.Store;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public abstract class ClientRequestHandler {

    private static final Logger logger = Logger.getLogger(VoldemortNativeRequestHandler.class);

    protected Store<ByteArray, byte[], byte[]> store;
    protected int protocolVersion;

    public ClientRequestHandler(Store<ByteArray, byte[], byte[]> store, int protocolVersion) {
        this.store = store;
        this.protocolVersion = protocolVersion;
    }

    public abstract boolean parseRequest(DataInputStream inputStream) throws IOException;

    public abstract void processRequest() throws VoldemortException;

    public abstract void writeResponse(DataOutputStream outputStream) throws IOException;
    
    public abstract int getResponseSize();

    public abstract String getDebugMessage();

    public static boolean skipByteArrayShort(DataInputStream inputStream, ByteBuffer buffer)
            throws VoldemortException, IOException {
        int dataSize = inputStream.readShort();
        return ByteUtils.skipByteArray(buffer, dataSize);

    }

    public static boolean skipByteArray(DataInputStream inputStream, ByteBuffer buffer)
            throws VoldemortException, IOException {
        int dataSize = inputStream.readInt();
        return ByteUtils.skipByteArray(buffer, dataSize);
    }

    public static ByteArray readKey(DataInputStream inputStream) throws IOException {
        int keySize = inputStream.readInt();
        byte[] key = new byte[keySize];
        inputStream.readFully(key);
        return new ByteArray(key);
    }

    public static byte[] readSingleTransform(DataInputStream inputStream, int protocolVersion)
            throws IOException {
        byte[] transforms = null;
        if(protocolVersion > 2) {
            if(inputStream.readBoolean())
                transforms = readTransforms(inputStream);
        }

        return transforms;
    }

    public static byte[] readTransforms(DataInputStream inputStream) throws IOException {
        int size = inputStream.readInt();
        if(size == 0)
            return null;
        byte[] transforms = new byte[size];
        inputStream.readFully(transforms);
        return transforms;
    }

    public static void writeResults(DataOutputStream outputStream, List<Versioned<byte[]>> values)
            throws IOException {
        outputStream.writeInt(values.size());
        for(Versioned<byte[]> v: values) {
            byte[] clock =  v.getVersion().toBytes();
            byte[] value = v.getValue();
            outputStream.writeInt(clock.length + value.length);
            outputStream.write(clock);
            outputStream.write(value);
        }
    }

    public static int getResultsSize(List<Versioned<byte[]>> values) {
        int size = 4;
        for(Versioned<byte[]> v: values) {
            size += 4;

            size += v.getVersion().sizeInBytes();
            size += v.getValue().length;
        }
        return size;
    }

    public static String getDebugMessageForKey(ByteArray key) {
        if(key == null || key.get() == null) {
            return "Key: ****Null***";
        } 
        else {
            byte[] keyBytes = key.get();
            if(keyBytes.length > 256) {
                keyBytes = Arrays.copyOfRange(keyBytes, 0, 256);
            }
            return " Key: " + ByteUtils.toHexString(keyBytes) + " KeySize "
               + key.length();
        }
    }

    public static String getDebugMessageForValue(List<Versioned<byte[]>> values) {
        long totalValueSize = 0;
        String valueSizeStr = "[";
        String valueHashStr = "[";
        String versionsStr = "[";
        for(Versioned<byte[]> b: values) {
            int len = b.getValue().length;
            totalValueSize += len;
            valueSizeStr += len + ",";
            valueHashStr += b.hashCode() + ",";
            versionsStr += b.getVersion();
        }
        valueSizeStr += "]";
        valueHashStr += "]";
        versionsStr += "]";

        return " numResults: " + values.size() + " totalResultSize: " + totalValueSize
               + " resultSizes: " + valueSizeStr + " resultHashes: " + valueHashStr + " versions: "
               + versionsStr;
    }

}
