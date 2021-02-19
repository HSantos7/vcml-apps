package voldemort.server.protocol.vold;

import voldemort.VoldemortException;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.utils.ByteUtils;
import voldemort.consistency.versioning.VectorClock;
import voldemort.consistency.versioning.Version;
import voldemort.store.Store;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


public class DeleteRequestHandler extends ClientRequestHandler {

    ByteArray key;
    Version version;
    boolean succeeded;

    public DeleteRequestHandler(Store<ByteArray, byte[], byte[]> store, int protocolVersion) {
        super(store, protocolVersion);
    }

    public static boolean isCompleteRequest(DataInputStream inputStream, ByteBuffer buffer)
            throws IOException, VoldemortException {
        // skip the key
        if(!ClientRequestHandler.skipByteArray(inputStream, buffer)) {
            return false;
        }
        // skip the version
        return ClientRequestHandler.skipByteArrayShort(inputStream, buffer);
    }

    @Override
    public boolean parseRequest(DataInputStream inputStream) throws IOException {
        key = ClientRequestHandler.readKey(inputStream);
        int versionSize = inputStream.readShort();
        byte[] versionBytes = new byte[versionSize];
        ByteUtils.read(inputStream, versionBytes);
        version = new VectorClock(versionBytes);
        return false;
    }

    @Override
    public void processRequest() throws VoldemortException {
        succeeded = store.delete(key, version);
    }

    @Override
    public void writeResponse(DataOutputStream outputStream) throws IOException {
        outputStream.writeShort(0);
        outputStream.writeBoolean(succeeded);
    }

    @Override
    public int getResponseSize() {
        return 2 + 1;
    }

    @Override
    public String getDebugMessage() {
        return "Operation DELETE " + ClientRequestHandler.getDebugMessageForKey(key)
               + " ClockSize " + version.sizeInBytes();
    }

}
