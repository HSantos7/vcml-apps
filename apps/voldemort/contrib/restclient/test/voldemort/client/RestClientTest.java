package voldemort.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.consistency.cluster.Cluster;
import voldemort.rest.coordinator.CoordinatorProxyService;
import voldemort.rest.coordinator.config.CoordinatorConfig;
import voldemort.rest.coordinator.config.FileBasedStoreClientConfigService;
import voldemort.rest.coordinator.config.StoreClientConfigService;
import voldemort.restclient.RESTClientFactory;
import voldemort.restclient.RESTClientFactoryConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.utils.SystemTime;
import voldemort.consistency.versioning.ObsoleteVersionException;
import voldemort.consistency.versioning.VectorClock;
import voldemort.consistency.versioning.Versioned;

public class RestClientTest extends DefaultStoreClientTest {

    private static final String STORE_NAME = "test";
    private static final String FAT_CLIENT_CONFIG_PATH_ORIGINAL = "test/common/coordinator/config/clientConfigs.avro";
    private static File COPY_OF_FAT_CLIENT_CONFIG_FILE;
    private static String storesXmlfile = "test/common/voldemort/config/two-stores.xml";

    String[] bootStrapUrls = null;
    private VoldemortServer[] servers;
    private CoordinatorProxyService coordinator;
    private Cluster cluster;
    public static String socketUrl = "";
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    @Override
    @Before
    public void setUp() throws IOException {
        final int numServers = 1;
        this.nodeId = 0;
        this.time = SystemTime.INSTANCE;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3, 4, 5, 6, 7 } };
        try {

            // Setup the cluster
            cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                            servers,
                                                            partitionMap,
                                                            socketStoreFactory,
                                                            true,
                                                            null,
                                                            storesXmlfile,
                                                            new Properties());

        } catch(IOException e) {
            fail("Failure to setup the cluster");
        }

        socketUrl = servers[0].getIdentityNode().getSocketUrl().toString();
        List<String> bootstrapUrls = new ArrayList<String>();
        bootstrapUrls.add(socketUrl);

        // create a copy of the config file in a temp directory and work on that
        File src = new File(FAT_CLIENT_CONFIG_PATH_ORIGINAL);
        COPY_OF_FAT_CLIENT_CONFIG_FILE = new File(TestUtils.createTempDir(),
                                                  "clientConfigs" + System.currentTimeMillis()
                                                          + ".avro");
        FileUtils.copyFile(src, COPY_OF_FAT_CLIENT_CONFIG_FILE);

        // Setup the Coordinator
        CoordinatorConfig coordinatorConfig = new CoordinatorConfig();
        coordinatorConfig.setBootstrapURLs(bootstrapUrls)
                         .setCoordinatorCoreThreads(100)
                         .setCoordinatorMaxThreads(100)
                         .setFatClientConfigPath(COPY_OF_FAT_CLIENT_CONFIG_FILE.getAbsolutePath())
                         .setServerPort(9999);

        try {
            StoreClientConfigService storeClientConfigs = null;
            switch(coordinatorConfig.getFatClientConfigSource()) {
                case FILE:
                    storeClientConfigs = new FileBasedStoreClientConfigService(coordinatorConfig);
                    break;
                case ZOOKEEPER:
                    throw new UnsupportedOperationException("Zookeeper-based configs are not implemented yet!");
                default:
                    storeClientConfigs = null;
            }
            coordinator = new CoordinatorProxyService(coordinatorConfig, storeClientConfigs);
            coordinator.start();
        } catch(Exception e) {
            e.printStackTrace();
            fail("Failure to start the Coordinator");
        }

        Properties props = new Properties();
        props.setProperty(ClientConfig.BOOTSTRAP_URLS_PROPERTY, "http://localhost:9999");
        props.setProperty(ClientConfig.ROUTING_TIMEOUT_MS_PROPERTY, "1500");

        RESTClientFactoryConfig mainConfig = new RESTClientFactoryConfig(props, null);
        RESTClientFactory factory = new RESTClientFactory(mainConfig);

        this.client = factory.getStoreClient(STORE_NAME);
    }

    @After
    public void tearDown() throws Exception {
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }

        coordinator.stop();

        // clean up the temporary file created in set up
        COPY_OF_FAT_CLIENT_CONFIG_FILE.delete();
    }

    @Override
    @Test
    public void testGetWithDefault() {
        assertEquals("GET of missing key should return default.",
                     new Versioned<String>("v"),
                     client.get("k", new Versioned<String>("v")));
        assertEquals("null should be an acceptable default value.",
                     null,
                     client.getValue("k", null));
        client.put("k", "v");
        VectorClock expectedVC = new VectorClock().incremented(nodeId, time.getMilliseconds());
        assertEquals("If there is a value for k, get(k) should return it.",
                     "v",
                     client.get("k", new Versioned<String>("v2")).getValue());
        assertNotNull(client.get("k").getVersion());
    }

    @Override
    @Test
    public void testPutVersioned() {
        VectorClock vc = new VectorClock();
        vc.incrementVersion(this.nodeId, System.currentTimeMillis());
        VectorClock initialVC = vc.clone();

        client.put("k", new Versioned<String>("v", vc));
        Versioned<String> v = client.get("k");
        assertEquals("GET should return the version set by PUT.", "v", v.getValue());

        VectorClock expected = initialVC.clone();
        expected.incrementVersion(this.nodeId, System.currentTimeMillis());
        assertEquals("The version should be incremented after a put.",
                     expected.getEntries(),
                     ((VectorClock) v.getVersion()).getEntries());
        try {
            client.put("k", new Versioned<String>("v", initialVC));
            fail("Put of obsolete version should throw exception.");
        } catch(ObsoleteVersionException e) {
            // this is good
        }
        // PUT of a concurrent version should succeed
        client.put("k",
                   new Versioned<String>("v2",
                                         new VectorClock().incremented(nodeId + 1,
                                                                       time.getMilliseconds())));
        assertEquals("GET should return the new value set by PUT.", "v2", client.getValue("k"));
        assertEquals("GET should return the new version set by PUT.",
                     expected.incremented(nodeId + 1, time.getMilliseconds()),
                     client.get("k").getVersion());
    }

    @Override
    @Test
    public void testPutIfNotObsolete() {
        VectorClock vc = new VectorClock();
        vc.incrementVersion(this.nodeId, System.currentTimeMillis());
        VectorClock initialVC = vc.clone();

        client.putIfNotObsolete("k", new Versioned<String>("v", vc));
        assertEquals("PUT of non-obsolete version should succeed.", "v", client.getValue("k"));
        assertFalse(client.putIfNotObsolete("k", new Versioned<String>("v2", initialVC)));
        assertEquals("Failed PUT should not change the value stored.", "v", client.getValue("k"));
    }

    @Override
    @Test
    public void testDeleteVersion() {
        VectorClock vc = new VectorClock();
        vc.incrementVersion(this.nodeId, System.currentTimeMillis());
        VectorClock initialVC = vc.clone();

        assertFalse("Delete of non-existant key should be false.", client.delete("k", vc));
        client.put("k", new Versioned<String>("v", vc));
        assertFalse("Delete of a lesser version should be false.", client.delete("k", initialVC));
        assertNotNull("After failed delete, value should still be there.", client.get("k"));
        assertTrue("Delete of k, with the current version should succeed.",
                   client.delete("k", initialVC.incremented(this.nodeId, time.getMilliseconds())));
        assertNull("After a successful delete(k), get(k) should return null.", client.get("k"));
    }
}
