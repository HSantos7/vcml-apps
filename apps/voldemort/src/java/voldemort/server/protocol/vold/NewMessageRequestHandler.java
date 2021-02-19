package voldemort.server.protocol.vold;

import voldemort.VoldemortException;
import voldemort.consistency.Constants;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.utils.ByteUtils;
import voldemort.consistency.versioning.Version;
import voldemort.consistency.versioning.Versioned;
import voldemort.store.Store;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

public class NewMessageRequestHandler extends ClientRequestHandler {

    ByteArray key;
    byte[] value;
    byte[] transforms;
    Version clock;

    public NewMessageRequestHandler(Store<ByteArray, byte[], byte[]> store, int protocolVersion) {
        super(store, protocolVersion);
    }
    
    public static boolean isCompleteRequest(DataInputStream inputStream,
                                            ByteBuffer buffer,
                                            int protocolVersion)
            throws IOException, VoldemortException {
        if(!ClientRequestHandler.skipByteArray(inputStream, buffer))
            return false;

        if(!ClientRequestHandler.skipByteArray(inputStream, buffer))
            return false;

        ClientRequestHandler.readSingleTransform(inputStream, protocolVersion);
        return true;
    }

    @Override
    public boolean parseRequest(DataInputStream inputStream) throws IOException {
        key = ClientRequestHandler.readKey(inputStream);
        int valueSize = inputStream.readInt();
        Class versionClass = null;
        try {
            versionClass = Class.forName(Constants.versioning);
            Method m = versionClass.getDeclaredMethod("createNew", DataInputStream.class);
            clock = (Version) m.invoke(null, inputStream);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        int vectorClockSize = clock.sizeInBytes();
        value = new byte[valueSize - vectorClockSize];
        ByteUtils.read(inputStream, value);

        transforms = ClientRequestHandler.readSingleTransform(inputStream, protocolVersion);
        return false;
    }

    @Override
    public void processRequest() throws VoldemortException {
        store.put(key, new Versioned<byte[]>(value, clock), transforms);
    }

    @Override
    public void writeResponse(DataOutputStream outputStream) throws IOException {
        outputStream.writeShort(0);
    }

    @Override
    public int getResponseSize() {
        return 2;
    }

    @Override
    public String getDebugMessage() {
        return "Operation PUT " + ClientRequestHandler.getDebugMessageForKey(key) + " ValueHash"
               + (value == null ? "null" : value.hashCode()) + " ClockSize " + clock.sizeInBytes()
               + " ValueSize " + (value == null ? "null" : value.length);
    }

}
