package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.ZooKeeperCluster;

public class SUTForBookkeeper extends BookKeeperClusterTestCase {


    private static SUTForBookkeeper instance;
    static final int ENS_SIZE = 3;
    static final int ZK_TIMEOUT = 100;
    private SUTForBookkeeper(int numBookies) {
        super(numBookies);
    }

    public ZooKeeperCluster getZooKeeperCluster() {
        return this.zkUtil;
    }

    /**
     * Getting sut env for testing
     * @param numBookies the number of bookies running in the SUT
     * @return the sut
     */
    public static synchronized SUTForBookkeeper getInstance(int numBookies) {
        if (instance == null) {
            instance = new SUTForBookkeeper(numBookies);
        }
        return instance;
    }

    public static ClientConfiguration getSUTforEvosuite() {
        SUTForBookkeeper sutForBookkeeper = new SUTForBookkeeper(3);
        ClientConfiguration conf = TestBKConfiguration.newClientConfiguration();
        conf.setMetadataServiceUri(sutForBookkeeper.getZooKeeperCluster().getMetadataServiceUri());
        conf.setZkTimeout(ZK_TIMEOUT);
        sutForBookkeeper = SUTForBookkeeper.getInstance(ENS_SIZE);
        try {
            sutForBookkeeper.setUp();
        } catch (Exception ignored) {
            // ignore
        }
        return conf;
    }
}
