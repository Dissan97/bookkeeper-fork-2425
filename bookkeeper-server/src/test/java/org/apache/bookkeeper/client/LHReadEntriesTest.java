package org.apache.bookkeeper.client;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LHReadEntriesTest extends LedgerHandleSUT{


    public static final List<byte[]> ENTRIES = IntStream.range(0, 3)
            .mapToObj(i -> ("entry" + i).getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        for (byte[] entry : ENTRIES) {
            lh.addEntry(entry);
        }
    }


    private int firstEntry;
    private int lastEntry;
    private AsyncCallback.ReadCallback cb;
    private Object ctx;
    private LHReadOutcome outcome;
    private final CompletableFuture<Enumeration<LedgerEntry>> result = new CompletableFuture<>();

    public LHReadEntriesTest(int firstEntry, int lastEntry, String cb, Object ctx, LHReadOutcome outcome) {
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.ctx = ctx;
        this.outcome = outcome;
        if (cb.equals("default")){
            this.cb = new SyncCallbackUtils.SyncReadCallback(result);
        }else if (cb.equals("invalid")) {
            this.cb = new InvalidReadCallback(result);
        }else {
            this.cb = null;
        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {-1, -2, "default", "ctx", LHReadOutcome.INCORRECT_PARAMETER},
                {-1, -1, "default", "ctx", LHReadOutcome.INCORRECT_PARAMETER},
                {-1, 2, "default", "ctx", LHReadOutcome.INCORRECT_PARAMETER},
                {0, -1, "default", "ctx", LHReadOutcome.INCORRECT_PARAMETER},
                {0, 0, "default", "ctx", LHReadOutcome.VALID},
                {0, 2, "default", "ctx", LHReadOutcome.VALID},
                {2, -1, "default", "ctx", LHReadOutcome.INCORRECT_PARAMETER},
                {2, 2, "default", "ctx", LHReadOutcome.VALID},
                {2, 3, "default", "ctx", LHReadOutcome.READ_EXCEPTION},
                {3, 2, "default", "ctx", LHReadOutcome.INCORRECT_PARAMETER},
                {3, 3, "default", "ctx", LHReadOutcome.READ_EXCEPTION},
                {3, 4, "default", "ctx", LHReadOutcome.READ_EXCEPTION},
                {0, 2, "invalid", "ctx", LHReadOutcome.INVALID_CB},
               // {0, 2, "null", "ctx", LHReadOutcome.NULL},
                {0, 2, "default", null, LHReadOutcome.VALID},

        });
    }

    @Test
    public void testReadEntries() {


        try {
            lh.asyncReadEntries(this.firstEntry, this.lastEntry, this.cb, this.ctx);
            Enumeration<LedgerEntry> entries = SyncCallbackUtils.waitForResult(result);

            while (entries.hasMoreElements()) {

                assertArrayEquals(ENTRIES.get(this.firstEntry++), entries.nextElement().getEntry());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (BKException e) {
            if (e.getCode() == BKException.Code.IllegalOpException) {
                assertEquals(LHReadOutcome.INVALID_CB, outcome);
            }else if (e.getCode() == BKException.Code.IncorrectParameterException) {
                assertEquals(LHReadOutcome.INCORRECT_PARAMETER, outcome);
            }else if (e.getCode() == BKException.Code.ReadException){
                assertEquals(LHReadOutcome.READ_EXCEPTION, outcome);
            }
        } catch (NullPointerException e) {
            assertEquals(LHReadOutcome.NULL, outcome);
            assertNull(this.cb);
        }

    }

    public static class InvalidReadCallback implements AsyncCallback.ReadCallback {

        private CompletableFuture<Enumeration<LedgerEntry>> future;

        public InvalidReadCallback(CompletableFuture<Enumeration<LedgerEntry>> future) {
            this.future = future;
        }

        @Override
        public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
            SyncCallbackUtils.finish(BKException.Code.IllegalOpException, seq, future);
        }
    }


    public enum LHReadOutcome {
        VALID,
        READ_EXCEPTION,
        NULL, INVALID_CB, INCORRECT_PARAMETER
    }

}
