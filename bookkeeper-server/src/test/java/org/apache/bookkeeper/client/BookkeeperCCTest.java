package org.apache.bookkeeper.client;


import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;


public class BookkeeperCCTest extends AbsBkCreate{


    @Test
    public void testNullLedgerCreation(){
        BookKeeper bkSpy = spy(bkClient);
        doAnswer(invocation -> {
            // Prendi il callback passato (parametro 6, quindi indice 5)
            AsyncCallback.CreateCallback cb = invocation.getArgument(5);
            Object ctx = invocation.getArgument(6);
            cb.createComplete(BKException.Code.OK, null, ctx);

            return null;
        }).when(bkSpy).asyncCreateLedger(
                eq(3), eq(3), eq(3),
                any(BookKeeper.DigestType.class), any(byte[].class),
                any(), any(), any()
        );

        doAnswer(invocation -> {
            // Prendi il callback passato (parametro 6, quindi indice 5)
            AsyncCallback.CreateCallback cb = invocation.getArgument(5);
            Object ctx = invocation.getArgument(6);
            cb.createComplete(BKException.Code.OK, null, ctx);

            return null;
        }).when(bkSpy).asyncCreateLedgerAdv(
                eq(3), eq(3), eq(3),
                any(BookKeeper.DigestType.class), any(byte[].class),
                any(), any(), any()
        );
        try {
            LedgerHandle lh = bkSpy.createLedger(3, 3, BookKeeper.DigestType.DUMMY, "password".getBytes());
            LedgerHandle lhAdv = bkSpy.createLedgerAdv(3,3, 3,
                    BookKeeper.DigestType.DUMMY, "password".getBytes());
            if (lh != null || lhAdv != null) {
                fail("Should have thrown an exception");
            }
        } catch (InterruptedException e) {
            fail();
        } catch (BKException e) {
            assertEquals(BKException.Code.UnexpectedConditionException, e.getCode());
        }
    }

    @Test
    public void testClientClosed(){
        LedgerHandle lhCreate = allThingsWorking();
        checkOpen();
        checkOpenAdv();
        checkCreate(lhCreate);
        checkOpenNoRecovery(lhCreate);
        checkDelete(lhCreate);
    }

    private void checkDelete(LedgerHandle lhCreate) {
        try {
            bkClient.deleteLedger(lhCreate.getId());
        } catch (InterruptedException | NullPointerException e) {
            fail(e.getMessage());
        } catch (BKException e) {
            assertEquals(BKException.Code.ClientClosedException, e.getCode());

        }
    }

    private void checkOpenNoRecovery(LedgerHandle lhCreate) {
        try {
            bkClient.openLedgerNoRecovery(lhCreate.getId(), BookKeeper.DigestType.DUMMY, "password".getBytes());
        } catch (BKException e) {
            assertEquals(BKException.Code.ClientClosedException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }
    }

    private void checkCreate(LedgerHandle lhCreate) {
        try {
            bkClient.openLedger(lhCreate.getId(), BookKeeper.DigestType.DUMMY, "password".getBytes());
        } catch (BKException e) {
            assertEquals(BKException.Code.ClientClosedException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }
    }

    private void checkOpenAdv() {
        try {
            bkClient.createLedgerAdv(3, 3, 3, BookKeeper.DigestType.DUMMY,
                    "password".getBytes());
        } catch ( InterruptedException e) {
            fail();
        } catch (BKException e) {
            assertEquals(BKException.Code.ClientClosedException, e.getCode());
        }
    }

    private void checkOpen() {
        try {
            bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());
        } catch ( InterruptedException e) {
            fail();
        } catch (BKException e) {
            assertEquals(BKException.Code.ClientClosedException, e.getCode());
        }
    }

    @NotNull
    private LedgerHandle allThingsWorking() {
        LedgerHandle lhCreate = null;
        LedgerHandle lhOpen;
        try {
            lhCreate = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());
            bkClient.deleteLedger(lhCreate.getId());
            lhCreate = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());
            lhOpen = bkClient.openLedger(lhCreate.getId(), BookKeeper.DigestType.DUMMY, "password".getBytes());
            assertEquals(lhCreate.getId(), lhOpen.getId());
            bkClient.close();
        } catch (BKException | InterruptedException e) {
            fail(e.getMessage());
        }
        return lhCreate;
    }

    @Test
    public void testOpenNoRecoveryLedger(){
        final byte[] pwd = "password".getBytes();
        LedgerHandle lhCreate;
        long iid = -1;
        LedgerHandle lhOpen = null;
        List<byte[]> entries = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            entries.add(("entry#"+ i).getBytes());
        }

        try {
             lhCreate = bkClient.createLedger(BookKeeper.DigestType.DUMMY, pwd);
             iid = lhCreate.getId();
             for (byte[] entry : entries) {
                 lhCreate.addEntry(entry);
             }
             lhCreate.close();
        } catch (BKException | InterruptedException e) {
            fail(e.getMessage());
        }
        try {
            lhOpen = bkClient.openLedgerNoRecovery(iid, BookKeeper.DigestType.DUMMY, pwd);
            Enumeration<LedgerEntry> lhEntries = lhOpen.readEntries(0, 2);
            for (byte[] entry : entries) {
                assertArrayEquals(entry, lhEntries.nextElement().getEntry());
            }
            CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
            SyncCallbackUtils.SyncOpenCallback result = new SyncCallbackUtils.SyncOpenCallback(future);
            bkClient.asyncOpenLedgerNoRecovery(iid, digestType, passwd,
                    result, null);
            lhOpen = SyncCallbackUtils.waitForResult(future);
            lhEntries = Objects.requireNonNull(lhOpen).readEntries(0, 2);
            for (byte[] entry : entries) {
                assertArrayEquals(entry, lhEntries.nextElement().getEntry());
            }
        } catch (BKException | InterruptedException e) {
            fail(e.getMessage());
        }

        try {
            lhOpen.addEntry("test".getBytes());
        } catch (BKException e) {
            assertEquals(BKException.Code.IllegalOpException, e.getCode());
        } catch (InterruptedException | NullPointerException e) {
            fail();
        }

        // addition openLedger when close


    }

    @Override
    @After
    public void tearDown() {

        try {
            super.tearDown();
        } catch (Exception ignored) {
            // ignore for this test suit
        }
    }

}
