/*
 * Copyright 2008-2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.Properties;

import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.consistency.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

public class ServerTestUtilsTest {

    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";
    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    @Test
    public void testStartVoldemortCluster() throws IOException {
        int numServers = 8;
        VoldemortServer[] servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0 }, { 1 }, { 2 }, { 3 }, { 4 }, { 5 }, { 6 }, { 7 } };
        Cluster cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                                servers,
                                                                partitionMap,
                                                                socketStoreFactory,
                                                                true,
                                                                null,
                                                                storesXmlfile,
                                                                new Properties());
        assertTrue(cluster != null);
    }

    // **********************************************************************
    // * START : "commented out" tests
    // These tests helped to find the root case of BindException problem when
    // clusters were started. These tests were used in debugging and stress
    // testing and should not be part of our general junit tests. The @Test
    // parameter is therefore commented out. The debugging methods themselves
    // are not commented out so that they can be kept up to date with other code
    // changes.

    // @Test
    public void stressTestStartVoldemortCluster() throws IOException {
        for(int i = 0; i < 10; i++) {
            testStartVoldemortCluster();
        }
    }

    // @Test
    public void startMultipleVoldemortServers() throws IOException {
        Cluster cluster = ServerTestUtils.getLocalCluster(16, new int[][] { { 0 }, { 1 }, { 2 },
                { 3 }, { 4 }, { 5 }, { 6 }, { 7 }, { 8 }, { 9 }, { 10 }, { 11 }, { 12 }, { 13 },
                { 14 }, { 15 } });

        VoldemortServer[] servers = new VoldemortServer[16];

        for(int i = 0; i < 16; i++) {
            servers[i] = ServerTestUtils.startVoldemortServer(socketStoreFactory,
                                                              ServerTestUtils.createServerConfig(true,
                                                                                                 i,
                                                                                                 TestUtils.createTempDir()
                                                                                                          .getAbsolutePath(),
                                                                                                 null,
                                                                                                 storesXmlfile,
                                                                                                 new Properties()),
                                                              cluster);
        }
        assertTrue(true);

        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
    }

    // @Test
    public void startMultipleVoldemortServersUnsafe5() throws IOException {
        for(int i = 0; i < 5; i++) {
            startMultipleVoldemortServers();
        }
        assertTrue(true);
    }

    // @Test
    public void startMultipleVoldemortServers10() {
        for(int i = 0; i < 10; i++) {
            boolean started = false;
            boolean caught = false;
            while(!started) {
                try {
                    startMultipleVoldemortServers();
                    started = true;
                } catch(IOException ioe) {
                    System.err.println("CAUGHT BIND ERROR! Trying again...");
                    ioe.printStackTrace();
                    caught = true;
                }
            }
            assertFalse(caught);
        }
    }

    // @Test
    public void testFindFreePort() throws Exception {
        ServerSocketChannel serverSocketChannel;
        try {
            serverSocketChannel = ServerSocketChannel.open();
        } catch(IOException ioe) {
            ioe.printStackTrace();
            assertTrue(false);
            return;
        }

        int port = ServerTestUtils.findFreePort();

        try {
            serverSocketChannel.socket().bind(new InetSocketAddress(port));
        } catch(IOException ioe) {
            ioe.printStackTrace();
            assertTrue(false);
            return;
        }

        assertTrue(true);
    }

    // @Test
    public void testFindFreePort1000() throws Exception {
        for(int i = 0; i < 1000; i++) {
            testFindFreePort();
        }
    }

    // @Test
    public void testFindFreePorts() throws Exception {
        int numPorts = 25000;
        ServerSocketChannel serverSocketChannel[] = new ServerSocketChannel[numPorts];
        int ports[] = ServerTestUtils.findFreePorts(numPorts);

        for(int i = 0; i < numPorts; i++) {
            boolean bound = false;
            while(!bound) {
                try {
                    serverSocketChannel[i] = ServerSocketChannel.open();
                    serverSocketChannel[i].socket().bind(new InetSocketAddress(ports[i]));
                    serverSocketChannel[i].socket().setReuseAddress(true);
                    bound = true;
                } catch(IOException ioe) {
                    System.err.println("Attempt: " + i + ", port: " + ports[i]);
                    ioe.printStackTrace();
                    Thread.sleep(10);
                }
            }
        }

        for(int i = 0; i < numPorts; i++) {
            try {
                serverSocketChannel[i].socket().close();
            } catch(IOException ioe) {
                System.err.println("Attempt: " + i + ", port: " + ports[i]);
                ioe.printStackTrace();
                assertTrue(false);
                return;
            }
        }

        assertTrue(true);
    }

    // @Test
    public void testFindFreePorts100() throws Exception {
        for(int i = 0; i < 100; i++) {
            System.out.println("testFindFreePorts100: " + i);
            testFindFreePorts();
        }
    }

    // * END : "commented out" tests
    // **********************************************************************
}
