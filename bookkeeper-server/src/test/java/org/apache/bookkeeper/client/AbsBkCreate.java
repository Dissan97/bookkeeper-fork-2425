package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.bookkeeper.client.SUTForBookkeeper.ENS_SIZE;
import static org.apache.bookkeeper.client.SUTForBookkeeper.ZK_TIMEOUT;

public class AbsBkCreate {
    private SUTForBookkeeper sut;
    protected final int ensSize;
    protected final int wQSize;
    protected final int aQSize;
    protected final BookKeeper.DigestType digestType;
    protected final byte[] passwd;
    protected final Map<String, byte[]> customMetadata;
    protected final BkCreationTest.TestOutcome testOutcome;
    protected BookKeeper bkClient;
    protected AtomicLong idCounter = new AtomicLong(0L);

    public AbsBkCreate(){
        this(3, 3, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), null,
                TestOutcome.VALID);
    };

    public AbsBkCreate(int ensSize, int wQSize, int aQSize, BookKeeper.DigestType digestType,
                       byte[] passwd, Map<String, byte[]> customMetadata, BkCreationTest.TestOutcome testOutcome) {
        this.ensSize = ensSize;
        this.wQSize = wQSize;
        this.aQSize = aQSize;
        this.digestType = digestType;
        this.passwd = passwd;
        this.customMetadata = customMetadata;
        this.testOutcome = testOutcome;
    }

    @BeforeClass
    public static void setUpSUT() throws Exception {

    }

    @Before
    public void setUp() throws Exception {
        sut = SUTForBookkeeper.getInstance(ENS_SIZE);
        sut.setUp();
        ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
        conf.setMetadataServiceUri(sut.getZooKeeperCluster().getMetadataServiceUri());
        conf.setZkTimeout(ZK_TIMEOUT);
        bkClient = new BookKeeper(conf);
    }

    @After
    public void tearDown() throws Exception {
        // cleanup
        try {
            this.bkClient.close();
        } catch (Exception ignored) {

        }
        sut.tearDown();
    }




    public enum TestOutcome {
        VALID,
        NULL,
        NOT_ENOUGH_BOOKIE,
        ILLEGAL_ARGUMENT,
        INVALID,
        EXCEPTION
    }
}
