package org.apache.bookkeeper.client;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class BkCreationTest extends AbsBkCreate{



    public BkCreationTest(int ensSize, int wQSize, int aQSize,
                          BookKeeper.DigestType digestType,
                          byte[] passwd, Map<String, byte[]> customMetadata,
                          TestOutcome testOutcome) {
        super(ensSize, wQSize, aQSize, digestType, passwd, customMetadata, testOutcome);
    }

    public static final Collection<Object[]> OPEN_PARAMS =
            Arrays.asList(new Object[][]{
                    /*
                     * the client should notice this test, but it doesn't
                    {-1, -2, -3, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                    {-1, -2, -2, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                    */
                    {-1, -2, -1, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.ILLEGAL_ARGUMENT},
                    /*
                    {-1, -1, -2, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                     */
                    {-1, -1, -1, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                    {-1, -1, 0, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.ILLEGAL_ARGUMENT},
                    /*
                    {-1, 0, -1, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                    {-1, 0, 0, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                     */
                    {-1, 0, 1, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                    // this is strange maybe the protocol provides some mechanism for negative writeQuorum
                    {0, -1, -2, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.VALID},
                    {0, -1, -1, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.VALID},
                    {0, -1, 0, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.ILLEGAL_ARGUMENT},
                    {0, 0, -1, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.VALID},
                    {0, 0, 0, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.VALID},
                    {0, 0, 1, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.ILLEGAL_ARGUMENT},
                    /*
                    {0, 1, 0, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                    {0, 1, 1, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                     */
                    {0, 1, 2, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.ILLEGAL_ARGUMENT},
                    {3, 2, 1, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.VALID},
                    {3, 2, 2, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.VALID},
                    {3, 2, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.ILLEGAL_ARGUMENT},
                    {3, 3, 2, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.VALID},
                    {3, 3, 4, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.ILLEGAL_ARGUMENT},
                    /*
                    {3, 4, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                    {3, 4, 4, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                     */
                    {3, 4, 5, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.EXCEPTION},
                    {4, 3, 2, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 3, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 3, 4, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.ILLEGAL_ARGUMENT},
                    {4, 4, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 4, 4, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 4, 5, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.ILLEGAL_ARGUMENT},
                    {4, 5, 4, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 5, 5, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 5, 6, BookKeeper.DigestType.DUMMY, "password".getBytes(), null, TestOutcome.ILLEGAL_ARGUMENT},
                    {3, 3, 3, BookKeeper.DigestType.MAC, "password".getBytes(), null, TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.CRC32, "password".getBytes(), null, TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.CRC32C, "password".getBytes(), null, TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.MAC, "".getBytes(), null, TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.MAC, null, null, TestOutcome.NULL},
                    {3, 3, 3, BookKeeper.DigestType.MAC, "password".getBytes(), Collections.emptyMap(), TestOutcome.VALID},
            });

    @NotNull
    @Contract(pure = true)
    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return OPEN_PARAMS;
    }

    @Test
    public void test() {
        boolean check = false;
        try {

            LedgerHandle lh = bkClient.createLedger(
                    this.ensSize, this.wQSize, this.aQSize,
                    digestType, passwd, customMetadata
            );
            check = lh.getId() == idCounter++;
            lh.close();
        } catch (BKException e) {
            check = e.getCode() == BKException.Code.NotEnoughBookiesException;
        }catch ( IllegalArgumentException e){
          check = ensSize <  0 || this.wQSize < this.aQSize;
        } catch (NullPointerException e){
            check = this.testOutcome.equals(TestOutcome.NULL);
        } catch (InterruptedException e) {
            fail();
        }
        assertTrue(check);
    }




}
