package org.apache.bookkeeper.client;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;


@RunWith(value = Parameterized.class)
public class LHAddEntryTest extends LedgerHandleSUT{


    private final byte[] data;
    private final int offset;
    private final int length;
    private final AsyncCallback.AddCallback cb;
    private final Object ctx;
    private final AddEntryOutcome outcome;

    public LHAddEntryTest(byte[] data, int offset, int length, AsyncCallback.AddCallback cb,
                          Object ctx, AddEntryOutcome outcome) {
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.cb = cb;
        this.ctx = ctx;
        this.outcome = outcome;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {null, 0 , -1, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.OUT_OF_BOUND},
                {null, 0 , 0, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.NULL},
                {null, 0 , 1, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.NULL},
                {"".getBytes(), 0 , -1, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.OUT_OF_BOUND},
                {"".getBytes(), 0 , 0, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.VALID},
                {"".getBytes(), 0 , 1, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.OUT_OF_BOUND},
                {"test".getBytes(), 0 , 3, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.VALID},
                {"test".getBytes(), 0 , 4, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.VALID},
                {"test".getBytes(), 0 , 5, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.OUT_OF_BOUND},
                {"test".getBytes(), 0 , 4, new InvalidAddCallback(), "ctx", AddEntryOutcome.INVALID_CB},
                {"test".getBytes(), 0 , 4, null, "ctx", AddEntryOutcome.NULL},
                {"test".getBytes(), 0 , 4, new SyncCallbackUtils.SyncAddCallback(), null, AddEntryOutcome.VALID},
                {"test".getBytes(), 1 , 4, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.OUT_OF_BOUND},
                {"test".getBytes(), -1 , 4, new SyncCallbackUtils.SyncAddCallback(), "ctx", AddEntryOutcome.OUT_OF_BOUND},
        });
    }

    @Test
    public void testAddEntry()  {


            try {
                lh.asyncAddEntry(data, offset, length, cb, ctx);
                long ledgerId = SyncCallbackUtils.waitForResult(( CompletableFuture<Long>)cb);
                // controllo che non sia nullo
                assertNotNull(cb);
                // controllo se effettivamente sto passando l'istanza corretta
                assertTrue(cb instanceof SyncCallbackUtils.SyncAddCallback);
                // mi aspetto che l'id sia 0 in quanto non ci sono entry nel ledger
                assertEquals(0L, ledgerId);
            } catch (InterruptedException e) {
                fail();
            } catch (BKException e) {
                if (e.getCode() == BKException.Code.IllegalOpException) {
                    assertEquals(AddEntryOutcome.INVALID_CB, outcome);
                    assertTrue(cb instanceof InvalidAddCallback);
                }else if (e.getCode() == BKException.Code.UnexpectedConditionException){
                    assertNull(cb);
                    assertEquals(AddEntryOutcome.NULL, outcome);
                }else {
                    fail();
                }
            } catch (NullPointerException e) {
                assertEquals(AddEntryOutcome.NULL, outcome);
            } catch (ArrayIndexOutOfBoundsException e) {
                assertEquals(AddEntryOutcome.OUT_OF_BOUND, outcome);
            }
            

    }

    public static class InvalidAddCallback extends CompletableFuture<Long> implements AsyncCallback.AddCallback {

        @Override
        public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
            SyncCallbackUtils.finish(BKException.Code.IllegalOpException, entryId, this);
        }
    }

    public enum AddEntryOutcome {
        VALID,
        INVALID_CB,
        NULL,
        OUT_OF_BOUND
    }
}
