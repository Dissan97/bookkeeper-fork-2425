package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.*;

import static org.apache.bookkeeper.client.AbsBkOpen.VALID_PASSWORD;
import static org.apache.bookkeeper.client.SUTForBookkeeper.ENS_SIZE;
import static org.apache.bookkeeper.client.SUTForBookkeeper.ZK_TIMEOUT;

public class AbsBkDelete {

    private static SUTForBookkeeper sut;
    protected BookKeeper bkClient;
    protected static final String CTX_OPEN = "TESTS OPEN CTX";

    protected final long iid;
    protected final TestOutcome outcome;
    protected long lastId;
    public AbsBkDelete(long iid, TestOutcome outcome) {
        this.iid = iid;
        this.outcome = outcome;
    }



    public enum TestOutcome {
        VALID_LEDGER_ID,
        BK_EXCEPTION,
        NULL,
        INVALID_LEDGER_ID
    }

    @Before
    public void setUp() throws Exception {
        sut = SUTForBookkeeper.getInstance(ENS_SIZE);
        sut.setUp();
        ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
        conf.setMetadataServiceUri(sut.getZooKeeperCluster().getMetadataServiceUri());
        conf.setZkTimeout(ZK_TIMEOUT);
        try (BookKeeper bkDummy = new BookKeeper(conf)) {
            lastId = bkDummy.createLedger(BookKeeper.DigestType.DUMMY, VALID_PASSWORD).getId();
        }
        conf = TestBKConfiguration.newClientConfiguration();
        conf.setMetadataServiceUri(sut.getZooKeeperCluster().getMetadataServiceUri());
        conf.setZkTimeout(ZK_TIMEOUT);
        bkClient = new BookKeeper(conf);
    }


    @After
    public  void tearDown() throws Exception {
        bkClient.close();
        sut.tearDown();
    }



}
