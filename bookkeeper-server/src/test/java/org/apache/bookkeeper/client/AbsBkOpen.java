package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.apache.bookkeeper.client.SUTForBookkeeper.ENS_SIZE;
import static org.apache.bookkeeper.client.SUTForBookkeeper.ZK_TIMEOUT;

public class AbsBkOpen {

    private static SUTForBookkeeper sut;
    protected BookKeeper bkClient;
    protected static final String CTX_OPEN = "TESTS OPEN CTX";
    protected static final byte[] VALID_PASSWORD = "Valid Password".getBytes();
    protected static final byte[] INVALID_PASSWORD = "Invalid Password".getBytes();

    protected static long validId;


    protected final long iid;
    protected final BookKeeper.DigestType digestType;
    protected final byte[] passwd;
    protected final BkOpenTest.TestOutcome outcome;

    public AbsBkOpen(long iid,
                     BookKeeper.DigestType digestType,
                      byte[] passwd,
                      BkOpenTest.TestOutcome outcome) {
        this.iid = iid;
        this.digestType = digestType;
        this.passwd = passwd;
        this.outcome = outcome;
    }



    @BeforeClass
    public static void setUpSUT() throws Exception {
        sut = SUTForBookkeeper.getInstance(ENS_SIZE);
        sut.setUp();
        ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
        conf.setMetadataServiceUri(sut.getZooKeeperCluster().getMetadataServiceUri());
        conf.setZkTimeout(ZK_TIMEOUT);
        BookKeeper bkDummy = new BookKeeper(conf);
        LedgerHandle lhDummy = bkDummy.createLedger(BookKeeper.DigestType.DUMMY, VALID_PASSWORD);
        validId = lhDummy.getId();
        lhDummy.close();
        bkDummy.close();
    }

    @Before
    public void setUp() throws Exception {
        ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
        conf.setMetadataServiceUri(sut.getZooKeeperCluster().getMetadataServiceUri());
        conf.setZkTimeout(ZK_TIMEOUT);
        bkClient = new BookKeeper(conf);
    }

    @After
    public void tearDown() throws Exception {
        // cleanup
        this.bkClient.close();
    }

    @AfterClass
    public static void tearDownSUT() throws Exception {
        sut.tearDown();
    }

    public enum TestOutcome {
        VALID,
        BK_EXCEPTION,
        NULL, INVALID_PASSWORD
    }
}
