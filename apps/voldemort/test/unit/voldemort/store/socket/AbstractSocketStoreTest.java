/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.store.socket;

import static org.junit.Assert.assertArrayEquals;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.client.protocol.RequestFormatType;
import voldemort.server.AbstractSocketService;
import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.versioning.Versioned;

/**
 * A base-socket store test that works with any store RequestFormat
 * 
 * 
 */
public abstract class AbstractSocketStoreTest extends AbstractByteArrayStoreTest {

    private static final Logger logger = Logger.getLogger(AbstractSocketStoreTest.class);

    public AbstractSocketStoreTest(RequestFormatType type, boolean useNio) {
        this.requestFormatType = type;
        this.useNio = useNio;
    }

    private int socketPort;
    private AbstractSocketService socketService;
    private Store<ByteArray, byte[], byte[]> socketStore;
    private final RequestFormatType requestFormatType;
    private final boolean useNio;
    private SocketStoreFactory socketStoreFactory;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.socketPort = ServerTestUtils.findFreePort();
        socketStoreFactory = new ClientRequestExecutorPool(2, 10000, 100000, 32 * 1024);
        socketService = ServerTestUtils.getSocketService(useNio,
                                                         VoldemortTestConstants.getOneNodeClusterXml(),
                                                         VoldemortTestConstants.getSimpleStoreDefinitionsXml(),
                                                         "test",
                                                         socketPort);
        socketService.start();
        socketStore = ServerTestUtils.getSocketStore(socketStoreFactory,
                                                     "test",
                                                     socketPort,
                                                     requestFormatType);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        socketService.stop();
        socketStore.close();
        socketStoreFactory.close();
    }

    @Override
    public Store<ByteArray, byte[], byte[]> getStore() {
        return socketStore;
    }

    @Test
    public void testVeryLargeValues() throws Exception {
        final Store<ByteArray, byte[], byte[]> store = getStore();
        byte[] biggie = new byte[1 * 1024 * 1024];
        ByteArray key = new ByteArray(biggie);
        Random rand = new Random();
        for(int i = 0; i < 10; i++) {
            rand.nextBytes(biggie);
            Versioned<byte[]> versioned = new Versioned<byte[]>(biggie);
            store.put(key, versioned, null);
            Versioned<byte[]> result = store.get(key, null).get(0);
            assertArrayEquals("Get returned different value after put",
                              result.getValue(),
                              versioned.getValue());

            assertTrue(store.delete(key, versioned.getVersion()));
        }
    }

    @Test
    public void testThreadOverload() throws Exception {
        final Store<ByteArray, byte[], byte[]> store = getStore();
        int numOps = 100;
        final CountDownLatch latch = new CountDownLatch(numOps);
        Executor exec = Executors.newCachedThreadPool();
        for(int i = 0; i < numOps; i++) {
            exec.execute(new Runnable() {

                public void run() {
                    try {
                        store.put(TestUtils.toByteArray(TestUtils.randomString("abcdefghijklmnopqrs",
                                                                               10)),
                                  new Versioned<byte[]>(TestUtils.randomBytes(8)),
                                  null);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
    }

    @Test
    public void testRepeatedClosedConnections() throws Exception {
        for(int i = 0; i < 100; i++) {
            Socket s = new Socket();
            s.setTcpNoDelay(true);
            s.setSoTimeout(1000);
            s.connect(new InetSocketAddress("localhost", socketPort));
            logger.info("Client opened" + i);
            // Thread.sleep(1);
            assertTrue(s.isConnected());
            assertTrue(s.isBound());
            assertTrue(!s.isClosed());
            s.close();
            logger.info("Client closed" + i);
        }
    }

    @Test
    public void testGetAllWithBigValueSizes() throws Exception {
        int[] keySizes = { 50, 100, 500, 1000, 5000, 10000 };
        int[] valueSizes = { 10000, 50000, 100000, 500000, 1000000, 2000000 };
        for(int i = 0; i < keySizes.length; i++) {
            logger.info("Testing with keySize = " + keySizes[i] + " and Value sizes: "
                               + valueSizes[i]);
            this.testGetAllWithBigValueSizes(getStore(), keySizes[i], valueSizes[i], 3);
        }
    }

    @Test
    public void testGetWithBigValueSizes() throws Exception {
        int[] keySizes = { 50, 100, 500, 1000, 5000, 10000 };
        int[] valueSizes = { 10000, 50000, 100000, 500000, 1000000, 2000000 };
        for(int i = 0; i < keySizes.length; i++) {
            logger.info("Testing with keySize = " + keySizes[i] + " and Value sizes: "
                               + valueSizes[i]);
            this.testGetWithBigValueSizes(getStore(), keySizes[i], valueSizes[i]);
        }
    }

}
