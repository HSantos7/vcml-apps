package voldemort.server.protocol.admin;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.client.protocol.pb.ProtoUtils;
import voldemort.client.protocol.pb.VAdminProto;
import voldemort.client.protocol.pb.VAdminProto.UpdatePartitionEntriesRequest;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.StreamRequestHandler;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StorageEngine;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.stats.StreamingStats;
import voldemort.store.stats.StreamingStats.Operation;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.utils.ByteUtils;
import voldemort.utils.EventThrottler;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Time;
import voldemort.utils.Utils;
import voldemort.consistency.versioning.ObsoleteVersionException;
import voldemort.consistency.versioning.Versioned;

/**
 * UpdatePartitionEntriesStreamRequestHandler implements the streaming logic for
 * updating partition entries.
 * 
 * This is the base class, which abstracts network IO to get an entry off the
 * wire, and provides a hook
 * {@link UpdatePartitionEntriesStreamRequestHandler#processEntry(ByteArray, Versioned)}
 * to implement custom logic (if needed) to manage how the entry will be written
 * to storage.
 * 
 * The default implementation of processEntry(..) simply issues a storage engine
 * put.
 * 
 */

public class UpdatePartitionEntriesStreamRequestHandler implements StreamRequestHandler {

    protected VAdminProto.UpdatePartitionEntriesRequest request;

    protected final VAdminProto.UpdatePartitionEntriesResponse.Builder responseBuilder = VAdminProto.UpdatePartitionEntriesResponse.newBuilder();

    protected final ErrorCodeMapper errorCodeMapper;

    protected final EventThrottler throttler;

    protected final VoldemortFilter filter;

    protected final StorageEngine<ByteArray, byte[], byte[]> storageEngine;

    protected final MetadataStore metadataStore;

    protected int counter;

    protected final long startTime;

    protected final StreamingStats streamStats;

    protected final Logger logger = Logger.getLogger(getClass());

    protected AtomicBoolean isBatchWriteOff;

    public UpdatePartitionEntriesStreamRequestHandler(UpdatePartitionEntriesRequest request,
                                                      ErrorCodeMapper errorCodeMapper,
                                                      VoldemortConfig voldemortConfig,
                                                      StorageEngine<ByteArray, byte[], byte[]> storageEngine,
                                                      StoreRepository storeRepository,
                                                      NetworkClassLoader networkClassLoader,
                                                      MetadataStore metadataStore) {
        super();
        this.request = request;
        this.errorCodeMapper = errorCodeMapper;
        this.storageEngine = storageEngine;
        this.metadataStore = metadataStore;
        throttler = new EventThrottler(voldemortConfig.getStreamMaxReadBytesPerSec());
        filter = (request.hasFilter()) ? AdminServiceRequestHandler.getFilterFromRequest(request.getFilter(),
                                                                                         voldemortConfig,
                                                                                         networkClassLoader)
                                      : new DefaultVoldemortFilter();
        startTime = System.currentTimeMillis();
        if(voldemortConfig.isJmxEnabled()) {
            this.streamStats = storeRepository.getStreamingStats(storageEngine.getName());
        } else {
            this.streamStats = null;
        }
        storageEngine.beginBatchModifications();
        isBatchWriteOff = new AtomicBoolean(false);
    }

    @Override
    public StreamRequestHandlerState handleRequest(DataInputStream inputStream,
                                                   DataOutputStream outputStream)
            throws IOException {
        if(!metadataStore.getPartitionStreamingEnabledUnlocked()) {
            throw new VoldemortException("Partition streaming is disabled on node "
                                         + metadataStore.getNodeId() + " under "
                                         + metadataStore.getServerStateUnlocked() + " state.");
        }
        long startNs = System.nanoTime();
        if(request == null) {
            int size = 0;
            try {
                size = inputStream.readInt();
            } catch(EOFException e) {
                if(logger.isTraceEnabled())
                    logger.trace("Incomplete read for message size");
                if(streamStats != null)
                    streamStats.reportNetworkTime(Operation.UPDATE_ENTRIES,
                                                  Utils.elapsedTimeNs(startNs, System.nanoTime()));
                return StreamRequestHandlerState.INCOMPLETE_READ;
            }

            if(size == -1) {
                long totalTime = (System.currentTimeMillis() - startTime) / 1000;
                logger.info(getHandlerName() + " successfully updated " + counter
                            + " entries for store '" + storageEngine.getName() + "' in "
                            + totalTime + " s");

                if(logger.isTraceEnabled())
                    logger.trace("Message size -1, completed partition update");
                if(streamStats != null)
                    streamStats.reportNetworkTime(Operation.UPDATE_ENTRIES,
                                                  Utils.elapsedTimeNs(startNs, System.nanoTime()));
                return StreamRequestHandlerState.COMPLETE;
            }

            if(logger.isTraceEnabled())
                logger.trace("UpdatePartitionEntriesRequest message size: " + size);

            byte[] input = new byte[size];

            try {
                ByteUtils.read(inputStream, input);
            } catch(EOFException e) {
                if(logger.isTraceEnabled())
                    logger.trace("Incomplete read for message");

                return StreamRequestHandlerState.INCOMPLETE_READ;
            } finally {
                if(streamStats != null)
                    streamStats.reportNetworkTime(Operation.UPDATE_ENTRIES,
                                                  Utils.elapsedTimeNs(startNs, System.nanoTime()));
            }

            VAdminProto.UpdatePartitionEntriesRequest.Builder builder = VAdminProto.UpdatePartitionEntriesRequest.newBuilder();
            builder.mergeFrom(input);
            request = builder.build();
        }

        VAdminProto.PartitionEntry partitionEntry = request.getPartitionEntry();
        ByteArray key = ProtoUtils.decodeBytes(partitionEntry.getKey());
        Versioned<byte[]> value = ProtoUtils.decodeVersioned(partitionEntry.getVersioned());

        if(filter.accept(key, value)) {
            startNs = System.nanoTime();
            try {
                processEntry(key, value);
                if(logger.isTraceEnabled())
                    logger.trace(getHandlerName() + " (Streaming put) successful");
            } catch(ObsoleteVersionException e) {
                // log and ignore
                if(logger.isDebugEnabled())
                    logger.debug(getHandlerName()
                                 + " (Streaming put) threw ObsoleteVersionException, Ignoring.");
            } finally {
                if(streamStats != null) {
                    streamStats.reportStreamingPut(Operation.UPDATE_ENTRIES);
                    streamStats.reportStorageTime(Operation.UPDATE_ENTRIES,
                                                  Utils.elapsedTimeNs(startNs, System.nanoTime()));
                }
            }
            throttler.maybeThrottle(key.length() + AdminServiceRequestHandler.valueSize(value));
        }
        // log progress
        counter++;
        if(0 == counter % STAT_RECORDS_INTERVAL) {
            long totalTime = (System.currentTimeMillis() - startTime) / Time.MS_PER_SECOND;
            logger.info(getHandlerName() + " updated " + counter + " entries for store '"
                        + storageEngine.getName() + "' in " + totalTime + " s");
        }

        request = null;
        return StreamRequestHandlerState.READING;
    }

    @Override
    public StreamRequestDirection getDirection() {
        return StreamRequestDirection.READING;
    }

    @Override
    public void close(DataOutputStream outputStream) throws IOException {
        ProtoUtils.writeMessage(outputStream, responseBuilder.build());
        storageEngine.endBatchModifications();
        isBatchWriteOff.compareAndSet(false, true);
    }

    @Override
    public void handleError(DataOutputStream outputStream, VoldemortException e) throws IOException {
        responseBuilder.setError(ProtoUtils.encodeError(errorCodeMapper, e));
        if(logger.isEnabledFor(Level.ERROR))
            logger.error(getHandlerName() + " handleUpdatePartitionEntries failed for request("
                         + request + ")", e);
    }

    @Override
    protected void finalize() {
        // when the object is GCed, don't forget to end the batch-write mode, if
        // not done already. After closer inspection of code, it seems like
        // close() will always be called. But, just doing this for safety.
        if(!isBatchWriteOff.get()) {
            storageEngine.endBatchModifications();
        }
    }

    @SuppressWarnings("unused")
    protected void processEntry(ByteArray key, Versioned<byte[]> value) throws IOException {
        storageEngine.put(key, value, null);
    }

    protected String getHandlerName() {
        return "UpdateEntries";
    }
}
