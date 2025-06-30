package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.junit.*;

import java.util.HashMap;

import static org.junit.Assert.*;

public class BkCreationLLMTest {

    private static SUTForBookkeeper sut;
    private static final int ENS_SIZE = 3;
    private static final int ZK_TIMEOUT = 100;
    private static int lastAddedLedger = 0;

    private LedgerHandle lh;
    private BookKeeper bkClient;

    @BeforeClass
    public static void setUpSUT() throws Exception {
        sut = SUTForBookkeeper.getInstance(ENS_SIZE);
        sut.setUp();
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
        this.bkClient.close();
    }

    @AfterClass
    public static void tearDownSUT() throws Exception {
        sut.tearDown();
    }

    // ✅ Success case: valid parameters
    @Test
    public void testValidParameters() throws Exception {
        byte[] passwd = "password".getBytes();
        HashMap<String, byte[]> metadata = new HashMap<>();
        lh = bkClient.createLedger(ENS_SIZE, 2, 2, DigestType.CRC32, passwd, metadata);
        assertNotNull(lh);
        lastAddedLedger++;
    }

    // ❌ writeQuorum < ackQuorum (should throw IllegalArgumentException)
    @Test(expected = IllegalArgumentException.class)
    public void testWriteQuorumLessThanAckQuorum() throws Exception {
        byte[] passwd = "password".getBytes();
        HashMap<String, byte[]> metadata = new HashMap<>();
        bkClient.createLedger(ENS_SIZE, 1, 2, DigestType.CRC32, passwd, metadata);
    }

    // ❌ null password
    @Test(expected = NullPointerException.class)
    public void testNullPassword() throws Exception {
        HashMap<String, byte[]> metadata = new HashMap<>();
        bkClient.createLedger(ENS_SIZE, 2, 2, DigestType.CRC32, null, metadata);
    }

    // ✅ Different digest type
    @Test
    public void testDifferentDigestTypes() throws Exception {
        byte[] passwd = "password".getBytes();
        HashMap<String, byte[]> metadata = new HashMap<>();

        for (DigestType dt : DigestType.values()) {
            lh = bkClient.createLedger(ENS_SIZE, 3, 3, dt, passwd, metadata);
            assertNotNull(lh);
            lastAddedLedger++;
        }
    }

    // ✅ Custom metadata
    @Test
    public void testWithCustomMetadata() throws Exception {
        byte[] passwd = "password".getBytes();
        HashMap<String, byte[]> metadata = new HashMap<>();
        metadata.put("creator", "tester".getBytes());

        lh = bkClient.createLedger(ENS_SIZE, 3, 3, DigestType.CRC32, passwd, metadata);
        assertNotNull(lh);
        lastAddedLedger++;
    }

    // ✅ ackQuorum = 1
    @Test
    public void testAckQuorumOne() throws Exception {
        byte[] passwd = "password".getBytes();
        HashMap<String, byte[]> metadata = new HashMap<>();

        lh = bkClient.createLedger(ENS_SIZE, 1, 1, DigestType.CRC32, passwd, metadata);
        assertNotNull(lh);
        lastAddedLedger++;
    }

    // ✅ writeQuorum = ackQuorum < ensembleSize
    @Test
    public void testWriteEqualsAckQuorumLessThanEnsembleSize() throws Exception {
        byte[] passwd = "password".getBytes();
        HashMap<String, byte[]> metadata = new HashMap<>();

        lh = bkClient.createLedger(ENS_SIZE, 2, 2, DigestType.CRC32, passwd, metadata);
        assertNotNull(lh);
        lastAddedLedger++;
    }

    // ✅ writeQuorum = ackQuorum = ensembleSize
    @Test
    public void testWriteEqualsAckEqualsEnsembleSize() throws Exception {
        byte[] passwd = "password".getBytes();
        HashMap<String, byte[]> metadata = new HashMap<>();

        lh = bkClient.createLedger(ENS_SIZE, ENS_SIZE, ENS_SIZE, DigestType.CRC32, passwd, metadata);
        assertNotNull(lh);
        lastAddedLedger++;
    }
}
