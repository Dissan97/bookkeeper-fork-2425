package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.Before;

import static org.apache.bookkeeper.client.SUTForBookkeeper.ENS_SIZE;
import static org.apache.bookkeeper.client.SUTForBookkeeper.ZK_TIMEOUT;

public class LedgerHandleSUT {

    protected SUTForBookkeeper sut;
    protected BookKeeper bkClient;
    protected LedgerHandle lh;
    protected long lhId;


    @Before
    public void setUp() throws Exception {
        sut = SUTForBookkeeper.getInstance(ENS_SIZE);
        sut.setUp();
        ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
        conf.setMetadataServiceUri(sut.getZooKeeperCluster().getMetadataServiceUri());
        conf.setZkTimeout(ZK_TIMEOUT);
        bkClient = new BookKeeper(conf);
        lh = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());
        lhId = lh.getId();
    }

    @After
    public void tearDown() throws Exception {
        sut.tearDown();
    }

}
