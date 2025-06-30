package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LLMOpenBkTest extends AbsBkOpen {

    public LLMOpenBkTest(long iid,
                         DigestType digestType,
                         byte[] passwd,
                         TestOutcome outcome) {
        super(iid, digestType, passwd, outcome);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { validId, DigestType.DUMMY, VALID_PASSWORD, TestOutcome.VALID },
                { validId, DigestType.DUMMY, INVALID_PASSWORD, TestOutcome.INVALID_PASSWORD },
                { 999999L, DigestType.DUMMY, VALID_PASSWORD, TestOutcome.BK_EXCEPTION },
                { -1L, DigestType.DUMMY, VALID_PASSWORD, TestOutcome.BK_EXCEPTION }
        });
    }

    @Test
    public void testOpenLedger() {
        try {
            LedgerHandle lh = bkClient.openLedger(iid, digestType, passwd);

            assertNotNull("LedgerHandle should not be null", lh);
            assertEquals("Ledger ID should match", iid, lh.getId());

            switch (outcome) {
                case VALID:
                    assertTrue(true);
                    break;
                case INVALID_PASSWORD:
                case BK_EXCEPTION:
                    fail("Expected exception for outcome: " + outcome);
                    break;
                default:
                    fail("Unexpected outcome type");
            }

            lh.close();
        } catch (Exception e) {
            switch (outcome) {
                case INVALID_PASSWORD:
                case BK_EXCEPTION:
                    assertTrue(e instanceof BKException || e instanceof IllegalArgumentException);
                    break;
                case VALID:
                    fail("Unexpected exception for VALID case: " + e);
                    break;
                default:
                    fail("Unhandled outcome: " + outcome);
            }
        }
    }
}
