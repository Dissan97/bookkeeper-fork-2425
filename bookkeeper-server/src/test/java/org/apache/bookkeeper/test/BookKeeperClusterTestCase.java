
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.test;

import com.google.common.base.Stopwatch;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.*;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.metastore.InMemoryMetaStore;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.bookkeeper.util.PortManager;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.junit.Assert.assertFalse;

/**
 * A class runs several bookie servers for testing.
 */
public abstract class BookKeeperClusterTestCase {

        static final Logger LOG = LoggerFactory.getLogger(BookKeeperClusterTestCase.class);

        @Rule
        public final TestName runtime = new TestName();

        @Rule
        public final Timeout globalTimeout;

        protected String testName;

        // Metadata service related variables
        protected final ZooKeeperCluster zkUtil;
        protected ZooKeeper zkc;
        protected String metadataServiceUri;

        // BookKeeper related variables
        protected final TmpDirs tmpDirs = new TmpDirs();
        protected final List<ServerTester> servers = new LinkedList<>();

        protected int numBookies;
        protected BookKeeperTestClient bkc;

        /*
         * Loopback interface is set as the listening interface and allowloopback is
         * set to true in this server config. So bookies in this test process would
         * bind to loopback address.
         */
        protected final ServerConfiguration baseConf = TestBKConfiguration.newServerConfiguration();
        protected final ClientConfiguration baseClientConf = TestBKConfiguration.newClientConfiguration();


        private boolean isAutoRecoveryEnabled;

        SynchronousQueue<Throwable> asyncExceptions = new SynchronousQueue<>();

        public BookKeeperClusterTestCase(int numBookies) {
                this(numBookies, TestConfig.TEST_TIMEOUT);
        }

        public BookKeeperClusterTestCase(int numBookies, int testTimeoutSecs) {
                this(numBookies, 1, testTimeoutSecs);
        }

        public BookKeeperClusterTestCase(int numBookies, int numOfZKNodes, int testTimeoutSecs) {
                this.numBookies = numBookies;
                this.globalTimeout = Timeout.seconds(testTimeoutSecs);
                if (numOfZKNodes == 1) {
                        zkUtil = new ZooKeeperUtil();
                } else {
                        try {
                                zkUtil = new ZooKeeperClusterUtil(numOfZKNodes);
                        } catch (IOException | KeeperException | InterruptedException e) {
                                throw new RuntimeException(e);
                        }
                }
        }

        @Before
        public void setUp() throws Exception {
                setUp("/ledgers");
        }

        @Before
        public void setTestNameJunit4() {
                testName = runtime.getMethodName();
        }



        protected void setUp(String ledgersRootPath) throws Exception {
                LOG.info("Setting up test {}", getClass());
                InMemoryMetaStore.reset();
                setMetastoreImplClass(baseConf);
                setMetastoreImplClass(baseClientConf);

                Stopwatch sw = Stopwatch.createStarted();
                try {
                        // start zookeeper service
                        startZKCluster();
                        // start bookkeeper service
                        this.metadataServiceUri = getMetadataServiceUri(ledgersRootPath);
                        startBKCluster(metadataServiceUri);
                        LOG.info("Setup testcase {} @ metadata service {} in {} ms.",
                                testName, metadataServiceUri,  sw.elapsed(TimeUnit.MILLISECONDS));
                } catch (Exception e) {
                        LOG.error("Error setting up", e);
                        throw e;
                }
        }

        protected String getMetadataServiceUri(String ledgersRootPath) {
                return zkUtil.getMetadataServiceUri(ledgersRootPath);
        }

        @After
        public void tearDown() throws Exception {
                boolean failed = false;
                for (Throwable e : asyncExceptions) {
                        LOG.error("Got async exception: ", e);
                        failed = true;
                }
                assertFalse("Async failure", failed);
                Stopwatch sw = Stopwatch.createStarted();
                LOG.info("TearDown");
                Exception tearDownException = null;
                // stop bookkeeper service
                try {
                        stopBKCluster();
                } catch (Exception e) {
                        LOG.error("Got Exception while trying to stop BKCluster", e);
                        tearDownException = e;
                }
                // stop zookeeper service
                try {
                        stopZKCluster();
                } catch (Exception e) {
                        LOG.error("Got Exception while trying to stop ZKCluster", e);
                        tearDownException = e;
                }
                // cleanup temp dirs
                try {
                        tmpDirs.cleanup();
                } catch (Exception e) {
                        LOG.error("Got Exception while trying to cleanupTempDirs", e);
                        tearDownException = e;
                }
                LOG.info("Tearing down test {} in {} ms.", testName, sw.elapsed(TimeUnit.MILLISECONDS));
                if (tearDownException != null) {
                        throw tearDownException;
                }
        }

        @After
        public void clearMetricsThreadRegistry() {
                ThreadRegistry.clear();
        }

        /**
         * Start zookeeper cluster.
         *
         * @throws Exception start Zookeeper problem
         */
        protected void startZKCluster() throws Exception {
                zkUtil.startCluster();
                zkc = zkUtil.getZooKeeperClient();
        }

        /**
         * Stop zookeeper cluster.
         *
         * @throws Exception stop zkCluster problems
         */
        protected void stopZKCluster() throws Exception {
                zkUtil.killCluster();
        }

        /**
         * Start cluster. Also, starts the auto recovery process for each bookie, if
         * isAutoRecoveryEnabled is true.
         *
         * @throws Exception
         */
        protected void startBKCluster(String metadataServiceUri) throws Exception {
                baseConf.setMetadataServiceUri(metadataServiceUri);
                baseClientConf.setMetadataServiceUri(metadataServiceUri);
                baseClientConf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
                if (numBookies > 0) {
                        bkc = new BookKeeperTestClient(baseClientConf, new TestStatsProvider());
                }

                // Create Bookie Servers (B1, B2, B3)
                for (int i = 0; i < numBookies; i++) {
                        startNewBookie();
                }
        }

        /**
         * Stop cluster. Also, stops all the auto recovery processes for the bookie
         * cluster, if isAutoRecoveryEnabled is true.
         *
         * @throws Exception
         */
        protected void stopBKCluster() throws Exception {
                if (bkc != null) {
                        bkc.close();
                }

                stopReplicationService();
                for (ServerTester t : servers) {
                        t.shutdown();
                }
                servers.clear();
        }

        protected ServerConfiguration newServerConfiguration() throws Exception {
                File f = tmpDirs.createNew("bookie", "test");

                int port;
                if (baseConf.isEnableLocalTransport() || !baseConf.getAllowEphemeralPorts()) {
                        port = PortManager.nextFreePort();
                } else {
                        port = 0;
                }
                return newServerConfiguration(port, f, new File[] { f });
        }

        protected ServerConfiguration newServerConfiguration(int port, File journalDir, File[] ledgerDirs) {
                ServerConfiguration conf = new ServerConfiguration(baseConf);
                conf.setBookiePort(port);
                conf.setJournalDirName(journalDir.getPath());
                String[] ledgerDirNames = new String[ledgerDirs.length];
                for (int i = 0; i < ledgerDirs.length; i++) {
                        ledgerDirNames[i] = ledgerDirs[i].getPath();
                }
                conf.setLedgerDirNames(ledgerDirNames);
                conf.setEnableTaskExecutionStats(true);
                conf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
                return conf;
        }

        protected void stopAllBookies() throws Exception {
                stopAllBookies(true);
        }

        protected void stopAllBookies(boolean shutdownClient) throws Exception {
                stopReplicationService();
                for (ServerTester t : servers) {
                        t.shutdown();
                }
                servers.clear();
                if (shutdownClient && bkc != null) {
                        bkc.close();
                        bkc = null;
                }
        }

        protected String newMetadataServiceUri(String ledgersRootPath) {
                return zkUtil.getMetadataServiceUri(ledgersRootPath);
        }

        protected String newMetadataServiceUri(String ledgersRootPath, String type) {
                return zkUtil.getMetadataServiceUri(ledgersRootPath, type);
        }

        /**
         * Get bookie address for bookie at index.
         */
        public BookieId getBookie(int index) throws Exception {
                return servers.get(index).getServer().getBookieId();
        }

        /**
         * Get bookie address for bookie at index.
         */
        public BookieServer getBookieServer(int index) throws Exception {
                return servers.get(index).getServer();
        }

        protected List<BookieId> bookieAddresses() throws Exception {
                List<BookieId> bookieIds = new ArrayList<>();
                for (ServerTester a : servers) {
                        bookieIds.add(a.getServer().getBookieId());
                }
                return bookieIds;
        }

        protected List<File> bookieLedgerDirs() throws Exception {
                return servers.stream()
                        .flatMap(t -> Arrays.stream(t.getConfiguration().getLedgerDirs()))
                        .collect(Collectors.toList());
        }

        protected List<File> bookieJournalDirs() throws Exception {
                return servers.stream()
                        .flatMap(t -> Arrays.stream(t.getConfiguration().getJournalDirs()))
                        .collect(Collectors.toList());
        }

        protected BookieId addressByIndex(int index) throws Exception {
                return servers.get(index).getServer().getBookieId();
        }

        protected BookieServer serverByIndex(int index) throws Exception {
                return servers.get(index).getServer();
        }

        protected ServerConfiguration confByIndex(int index) throws Exception {
                return servers.get(index).getConfiguration();
        }

        private Optional<ServerTester> byAddress(BookieId addr) throws UnknownHostException {
                for (ServerTester s : servers) {
                        if (s.getServer().getBookieId().equals(addr)) {
                                return Optional.of(s);
                        }
                }
                return Optional.empty();
        }

        protected int indexOfServer(BookieServer b) throws Exception {
                for (int i = 0; i < servers.size(); i++) {
                        if (servers.get(i).getServer().equals(b)) {
                                return i;
                        }
                }
                return -1;
        }

        protected int lastBookieIndex() {
                return servers.size() - 1;
        }

        protected int bookieCount() {
                return servers.size();
        }

        private OptionalInt indexByAddress(BookieId addr) throws UnknownHostException {
                for (int i = 0; i < servers.size(); i++) {
                        if (addr.equals(servers.get(i).getServer().getBookieId())) {
                                return OptionalInt.of(i);
                        }
                }
                return OptionalInt.empty();
        }

        /**
         * Get bookie configuration for bookie.
         */
        public ServerConfiguration getBkConf(BookieId addr) throws Exception {
                return byAddress(addr).get().getConfiguration();
        }

        /**
         * Kill a bookie by its socket address. Also, stops the autorecovery process
         * for the corresponding bookie server, if isAutoRecoveryEnabled is true.
         *
         * @param addr
         *            Socket Address
         * @return the configuration of killed bookie
         * @throws InterruptedException
         */
        public ServerConfiguration killBookie(BookieId addr) throws Exception {
                Optional<ServerTester> tester = byAddress(addr);
                if (tester.isPresent()) {
                        if (tester.get().getAutoRecovery() != null
                                && tester.get().getAutoRecovery().getAuditor() != null
                                && tester.get().getAutoRecovery().getAuditor().isRunning()) {
                                LOG.warn("Killing bookie {} who is the current Auditor", addr);
                        }
                        servers.remove(tester.get());
                        tester.get().shutdown();
                        return tester.get().getConfiguration();
                }
                return null;
        }

        /**
         * Set the bookie identified by its socket address to readonly.
         *
         * @param addr
         *          Socket Address
         * @throws InterruptedException
         */
        public void setBookieToReadOnly(BookieId addr) throws Exception {
                Optional<ServerTester> tester = byAddress(addr);
                if (tester.isPresent()) {
                        tester.get().getServer().getBookie().getStateManager().transitionToReadOnlyMode().get();
                }
        }

        /**
         * Kill a bookie by index. Also, stops the respective auto recovery process
         * for this bookie, if isAutoRecoveryEnabled is true.
         *
         * @param index
         *            Bookie Index
         * @return the configuration of killed bookie
         * @throws InterruptedException
         * @throws IOException
         */
        public ServerConfiguration killBookie(int index) throws Exception {
                ServerTester tester = servers.remove(index);
                tester.shutdown();
                return tester.getConfiguration();
        }

        /**
         * Kill bookie by index and verify that it's stopped.
         *
         * @param index index of bookie to kill
         *
         * @return configuration of killed bookie
         */
        public ServerConfiguration killBookieAndWaitForZK(int index) throws Exception {
                ServerTester tester = servers.get(index); // IKTODO: this method is awful
                ServerConfiguration ret = killBookie(index);
                while (zkc.exists(ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf) + "/" + AVAILABLE_NODE + "/"
                        + tester.getServer().getBookieId().toString(), false) != null) {
                        Thread.sleep(500);
                }
                return ret;
        }

        /**
         * Sleep a bookie.
         *
         * @param addr
         *          Socket Address
         * @param seconds
         *          Sleep seconds
         * @return Count Down latch which will be counted down just after sleep begins
         * @throws InterruptedException
         * @throws IOException
         */
        public CountDownLatch sleepBookie(BookieId addr, final int seconds)
                throws Exception {
                Optional<ServerTester> tester = byAddress(addr);
                if (tester.isPresent()) {
                        CountDownLatch latch = new CountDownLatch(1);
                        Thread sleeper = new Thread() {
                                @Override
                                public void run() {
                                        try {
                                                tester.get().getServer().suspendProcessing();
                                                LOG.info("bookie {} is asleep", tester.get().getAddress());
                                                latch.countDown();
                                                Thread.sleep(seconds * 1000);
                                                tester.get().getServer().resumeProcessing();
                                                LOG.info("bookie {} is awake", tester.get().getAddress());
                                        } catch (Exception e) {
                                                LOG.error("Error suspending bookie", e);
                                        }
                                }
                        };
                        sleeper.start();
                        return latch;
                } else {
                        throw new IOException("Bookie not found");
                }
        }

        /**
         * Sleep a bookie until I count down the latch.
         *
         * @param addr
         *          Socket Address
         * @param l
         *          Latch to wait on
         * @throws InterruptedException
         * @throws IOException
         */
        public void sleepBookie(BookieId addr, final CountDownLatch l)
                throws InterruptedException, IOException {
                final CountDownLatch suspendLatch = new CountDownLatch(1);
                sleepBookie(addr, l, suspendLatch);
                suspendLatch.await();
        }

        public void sleepBookie(BookieId addr, final CountDownLatch l, final CountDownLatch suspendLatch)
                throws InterruptedException, IOException {
                Optional<ServerTester> tester = byAddress(addr);
                if (tester.isPresent()) {
                        BookieServer bookie = tester.get().getServer();
                        LOG.info("Sleep bookie {}.", addr);
                        Thread sleeper = new Thread(() -> {
                                try {
                                        bookie.suspendProcessing();
                                        if (null != suspendLatch) {
                                                suspendLatch.countDown();
                                        }
                                        l.await();
                                        bookie.resumeProcessing();
                                } catch (Exception e) {
                                        LOG.error("Error suspending bookie", e);
                                }
                        });
                        sleeper.start();
                } else {
                        throw new IOException("Bookie not found");
                }
        }

        /**
         * Restart bookie servers. Also restarts all the respective auto recovery
         * process, if isAutoRecoveryEnabled is true.
         *
         * @throws InterruptedException
         * @throws IOException
         * @throws KeeperException
         * @throws BookieException
         */
        public void restartBookies()
                throws Exception {
                restartBookies(c -> c);
        }

        /**
         * Restart a bookie. Also restart the respective auto recovery process,
         * if isAutoRecoveryEnabled is true.
         *
         * @param addr
         * @throws InterruptedException
         * @throws IOException
         * @throws KeeperException
         * @throws BookieException
         */
        public void restartBookie(BookieId addr) throws Exception {
                OptionalInt toRemove = indexByAddress(addr);
                if (toRemove.isPresent()) {
                        ServerConfiguration newConfig = killBookie(toRemove.getAsInt());
                        Thread.sleep(1000);
                        startAndAddBookie(newConfig);
                } else {
                        throw new IOException("Bookie not found");
                }
        }

        public void restartBookies(Function<ServerConfiguration, ServerConfiguration> reconfFunction)
                throws Exception {
                // shut down bookie server
                List<ServerConfiguration> confs = new ArrayList<>();
                stopReplicationService();
                for (ServerTester server : servers) {
                        server.shutdown();
                        confs.add(server.getConfiguration());
                }
                servers.clear();
                Thread.sleep(1000);
                // restart them to ensure we can't
                for (ServerConfiguration conf : confs) {
                        // ensure the bookie port is loaded correctly
                        startAndAddBookie(reconfFunction.apply(conf));
                }
        }

        /**
         * Helper method to startup a new bookie server with the indicated port
         * number. Also, starts the auto recovery process, if the
         * isAutoRecoveryEnabled is set true.
         *
         * @throws IOException
         */
        public int startNewBookie()
                throws Exception {
                return startNewBookieAndReturnAddress().getPort();
        }

        public BookieSocketAddress startNewBookieAndReturnAddress()
                throws Exception {
                ServerConfiguration conf = newServerConfiguration();
                LOG.info("Starting new bookie on port: {}", conf.getBookiePort());
                return startAndAddBookie(conf).getServer().getLocalAddress();
        }

        public BookieId startNewBookieAndReturnBookieId()
                throws Exception {
                ServerConfiguration conf = newServerConfiguration();
                LOG.info("Starting new bookie on port: {}", conf.getBookiePort());
                return startAndAddBookie(conf).getServer().getBookieId();
        }

        protected ServerTester startAndAddBookie(ServerConfiguration conf) throws Exception {
                ServerTester server = startBookie(conf);
                servers.add(server);
                return server;
        }

        protected ServerTester startAndAddBookie(ServerConfiguration conf, Bookie b) throws Exception {
                ServerTester server = startBookie(conf, b);
                servers.add(server);
                return server;
        }
        /**
         * Helper method to startup a bookie server using a configuration object.
         * Also, starts the auto recovery process if isAutoRecoveryEnabled is true.
         *
         * @param conf
         *            Server Configuration Object
         *
         */
        protected ServerTester startBookie(ServerConfiguration conf)
                throws Exception {
                ServerTester tester = new ServerTester(conf);

                if (bkc == null) {
                        bkc = new BookKeeperTestClient(baseClientConf, new TestStatsProvider());
                }

                BookieId address = tester.getServer().getBookieId();
                /*Future<?> waitForBookie = conf.isForceReadOnlyBookie()
                        ? bkc.waitForReadOnlyBookie(address)
                        : bkc.waitForWritableBookie(address);

                 */

                tester.getServer().start();

                //waitForBookie.get(30, TimeUnit.SECONDS);
                LOG.info("New bookie '{}' has been created.", address);

                if (isAutoRecoveryEnabled()) {
                        tester.startAutoRecovery();
                }
                return tester;
        }

        /**
         * Start a bookie with the given bookie instance. Also, starts the auto
         * recovery for this bookie, if isAutoRecoveryEnabled is true.
         */
        protected ServerTester startBookie(ServerConfiguration conf, final Bookie b)
                throws Exception {
                ServerTester tester = new ServerTester(conf, b);
                if (bkc == null) {
                        bkc = new BookKeeperTestClient(baseClientConf, new TestStatsProvider());
                }
                BookieId address = tester.getServer().getBookieId();
                Future<?> waitForBookie = conf.isForceReadOnlyBookie()
                        ? bkc.waitForReadOnlyBookie(address)
                        : bkc.waitForWritableBookie(address);

                tester.getServer().start();

                waitForBookie.get(30, TimeUnit.SECONDS);
                LOG.info("New bookie '{}' has been created.", address);

                if (isAutoRecoveryEnabled()) {
                        tester.startAutoRecovery();
                }
                return tester;

        }

        public void setMetastoreImplClass(AbstractConfiguration conf) {
                conf.setMetastoreImplClass(InMemoryMetaStore.class.getName());
        }

        /**
         * Flags used to enable/disable the auto recovery process. If it is enabled,
         * starting the bookie server will starts the auto recovery process for that
         * bookie. Also, stopping bookie will stops the respective auto recovery
         * process.
         *
         * @param isAutoRecoveryEnabled
         *            Value true will enable the auto recovery process. Value false
         *            will disable the auto recovery process
         */
        public void setAutoRecoveryEnabled(boolean isAutoRecoveryEnabled) {
                this.isAutoRecoveryEnabled = isAutoRecoveryEnabled;
        }

        /**
         * Flag used to check whether auto recovery process is enabled/disabled. By
         * default the flag is false.
         *
         * @return true, if the auto recovery is enabled. Otherwise return false.
         */
        public boolean isAutoRecoveryEnabled() {
                return isAutoRecoveryEnabled;
        }

        /**
         * Will starts the auto recovery process for the bookie servers. One auto
         * recovery process per each bookie server, if isAutoRecoveryEnabled is
         * enabled.
         */
        public void startReplicationService() throws Exception {
                for (ServerTester t : servers) {
                        t.startAutoRecovery();
                }
        }

        /**
         * Will stops all the auto recovery processes for the bookie cluster, if
         * isAutoRecoveryEnabled is true.
         */
        public void stopReplicationService() throws Exception{
                for (ServerTester t : servers) {
                        t.stopAutoRecovery();
                }
        }

        /**
         * Check whether the InetSocketAddress was created using a hostname or an IP
         * address. Represent as 'hostname/IPaddress' if the InetSocketAddress was
         * created using hostname. Represent as '/IPaddress' if the
         * InetSocketAddress was created using an IPaddress
         *
         * @param bookieId id
         * @return true if the address was created using an IP address, false if the
         *         address was created using a hostname
         */
        public boolean isCreatedFromIp(BookieId bookieId) {
                BookieSocketAddress addr = bkc.getBookieAddressResolver().resolve(bookieId);
                return addr.getSocketAddress().toString().startsWith("/");
        }

        public void resetBookieOpLoggers() {
                servers.forEach(t -> t.getStatsProvider().clear());
        }

        public TestStatsProvider getStatsProvider(BookieId addr) throws UnknownHostException {
                return byAddress(addr).get().getStatsProvider();
        }

        public TestStatsProvider getStatsProvider(int index) throws Exception {
                return servers.get(index).getStatsProvider();
        }

        /**
         * Class to encapsulate all the test objects.
         */

}