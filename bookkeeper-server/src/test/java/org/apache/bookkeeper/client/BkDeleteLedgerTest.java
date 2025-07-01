package org.apache.bookkeeper.client;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class BkDeleteLedgerTest extends AbsBkDelete{

    public BkDeleteLedgerTest(long iid, TestOutcome outcome) {
        super(iid, outcome);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {0, TestOutcome.VALID_LEDGER_ID},
                {-1, TestOutcome.INVALID_LEDGER_ID},
                {Integer.MAX_VALUE, TestOutcome.INVALID_LEDGER_ID},
        });
    }

    @Test
    public void testDeleteLedger() throws Exception {

        try {
            bkClient.deleteLedger(iid);
            assertEquals(lastId,
                    bkClient.openLedger(lastId, BookKeeper.DigestType.DUMMY, AbsBkOpen.VALID_PASSWORD).getId());
        } catch (BKException e) {
            assertTrue(e.getCode() == BKException.Code.NoSuchLedgerExistsOnMetadataServerException
            && outcome.equals(TestOutcome.VALID_LEDGER_ID));
        }


    }


}
