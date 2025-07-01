package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.Before;
import org.junit.Test;

import static org.apache.bookkeeper.client.SUTForBookkeeper.ENS_SIZE;
import static org.apache.bookkeeper.client.SUTForBookkeeper.ZK_TIMEOUT;
import static org.junit.Assert.*;


public class BookkeeperMutationTest {

    private BookKeeper bkClient;
    private ClientConfiguration conf;
    @Before
    public void setUp() throws Exception {
        SUTForBookkeeper sut = SUTForBookkeeper.getInstance(ENS_SIZE);
        sut.setUp();
        conf = TestBKConfiguration.newClientConfiguration();
        conf.setMetadataServiceUri(sut.getZooKeeperCluster().getMetadataServiceUri());
        conf.setZkTimeout(ZK_TIMEOUT);
        bkClient = new BookKeeper(conf);

    }

    @Test
    public void testClientCloseMutation() throws InterruptedException {
        LedgerHandle lh = null;
        try {
            lh = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());
            bkClient.close();
            assertTrue(bkClient.isClosed());
            bkClient.isClosed(lh.getId());
        } catch (BKException e) {
            assertEquals(BKException.Code.ClientClosedException, e.getCode());
        }


    }


}
