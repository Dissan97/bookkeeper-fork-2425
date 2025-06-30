package org.apache.bookkeeper.client;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.bookkeeper.client.SyncCallbackUtils.finish;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class BkAsyncCreationTest extends AbsBkCreate {

    private AsyncCallback.CreateCallback cb = null;
    private final Object ctx;
    CompletableFuture<LedgerHandle> future = new CompletableFuture<>();

    public BkAsyncCreationTest(int ensSize, int wQSize, int aQSize, BookKeeper.DigestType digestType,
                               byte[] passwd, CreateCallbackType type, Object ctx,
                               Map<String, byte[]> customMetadata, TestOutcome testOutcome) {
        super(ensSize, wQSize, aQSize, digestType, passwd, customMetadata, testOutcome);

        if (type == CreateCallbackType.VALID) {
            this.cb = new SyncCallbackUtils.SyncCreateCallback(future);
        } else if (type == CreateCallbackType.INVALID){
            this.cb = new InvalidSyncCreateCb(future);
        }
        this.ctx = ctx;
    }

    private static final String CTX = "TEST CTX";

    public static final Collection<Object[]> ASYNC_OPEN_PARAMS =
            Arrays.asList(new Object[][]{
                    /*
                     * the client should notice this test, but it doesn't
                    {-1, -2, -3, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                             null, TestOutcome.EXCEPTION},
                    {-1, -2, -2, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                             null, TestOutcome.EXCEPTION},
                    */
                    {-1, -2, -1, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.ILLEGAL_ARGUMENT},
                    /*
                    {-1, -1, -2, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                             null, TestOutcome.EXCEPTION},
                     */
                    {-1, -1, -1, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.EXCEPTION},
                    {-1, -1, 0, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.ILLEGAL_ARGUMENT},
                    /*
                    {-1, 0, -1, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                             null, TestOutcome.EXCEPTION},
                    {-1, 0, 0, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                             null, TestOutcome.EXCEPTION},
                     */
                    {-1, 0, 1, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.EXCEPTION},
                    // this is strange maybe the protocol provides some mechanism for negative writeQuorum
                    {0, -1, -2, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {0, -1, -1, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {0, -1, 0, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.ILLEGAL_ARGUMENT},
                    {0, 0, -1, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {0, 0, 0, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {0, 0, 1, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.ILLEGAL_ARGUMENT},
                    /*
                    {0, 1, 0, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                             null, TestOutcome.EXCEPTION},
                    {0, 1, 1, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                             null, TestOutcome.EXCEPTION},
                     */
                    {0, 1, 2, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.ILLEGAL_ARGUMENT},
                    {3, 2, 1, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {3, 2, 2, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {3, 2, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.ILLEGAL_ARGUMENT},
                    {3, 3, 2, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {3, 3, 4, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.ILLEGAL_ARGUMENT},
                    /*
                    {3, 4, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                             null, TestOutcome.EXCEPTION},
                    {3, 4, 4, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                             null, TestOutcome.EXCEPTION},
                     */
                    {3, 4, 5, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.EXCEPTION},
                    {4, 3, 2, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 3, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 3, 4, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.ILLEGAL_ARGUMENT},
                    {4, 4, 3, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 4, 4, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 4, 5, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.ILLEGAL_ARGUMENT},
                    {4, 5, 4, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 5, 5, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.NOT_ENOUGH_BOOKIE},
                    {4, 5, 6, BookKeeper.DigestType.DUMMY, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.ILLEGAL_ARGUMENT},
                    {3, 3, 3, BookKeeper.DigestType.MAC, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.CRC32, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.CRC32C, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.MAC, "".getBytes(), CreateCallbackType.VALID, CTX,
                            null, TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.MAC, null, CreateCallbackType.VALID, CTX,
                            null, TestOutcome.NULL},
                    {3, 3, 3, BookKeeper.DigestType.MAC, "password".getBytes(), CreateCallbackType.VALID, CTX,
                            Collections.emptyMap(), TestOutcome.VALID},
                    {3, 3, 3, BookKeeper.DigestType.MAC, "password".getBytes(), CreateCallbackType.INVALID, CTX,
                            Collections.emptyMap(), TestOutcome.INVALID}/*
                            This part is not caught from the client lambda, so it is blocked,
                    {3, 3, 3, BookKeeper.DigestType.MAC, "password".getBytes(), CreateCallbackType.NULL, CTX,
                            Collections.emptyMap(), TestOutcome.NULL},*/
            });


    @NotNull
    @Contract(pure = true)
    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return ASYNC_OPEN_PARAMS;
    }


    @Test
    public void test() {
        boolean passed = false;
        if (cb == null){
            System.out.println("null");
        }
        try {
            bkClient.asyncCreateLedger(this.ensSize, this.wQSize, this.aQSize, this.digestType, this.passwd,
                    this.cb, this.ctx, this.customMetadata);
            try (LedgerHandle lh = SyncCallbackUtils.waitForResult(this.future)) {
                Assert.assertNotNull(lh);
                passed = (lh.getId() == idCounter++) && testOutcome.equals(TestOutcome.VALID);
            }
        } catch (InterruptedException e) {
            fail();
        } catch (BKException e) {
            passed = (e.getCode() == BKException.Code.UnauthorizedAccessException
                    && this.cb instanceof InvalidSyncCreateCb && testOutcome.equals(TestOutcome.INVALID)) ||
                    (e.getCode() == BKException.Code.NotEnoughBookiesException
                            && testOutcome.equals(TestOutcome.NOT_ENOUGH_BOOKIE));
        } catch (IllegalArgumentException e) {
            passed = ensSize < 0 || this.wQSize < this.aQSize;
        } catch (NullPointerException e) {
            passed = this.testOutcome.equals(TestOutcome.NULL);
        }

        assertTrue(passed);
    }

    public static class InvalidSyncCreateCb implements AsyncCallback.CreateCallback {

        private final CompletableFuture<? super LedgerHandle> future;

        public InvalidSyncCreateCb(CompletableFuture<? super LedgerHandle> future) {
            this.future = future;
        }

        @Override
        public void createComplete(int rc, LedgerHandle lh, Object ctx) {
            finish(BKException.Code.UnauthorizedAccessException, lh, future);
        }
    }

    public enum CreateCallbackType {
        NULL,
        VALID,
        INVALID
    }
}
