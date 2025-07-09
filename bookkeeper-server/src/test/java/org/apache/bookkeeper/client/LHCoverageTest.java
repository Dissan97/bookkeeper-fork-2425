package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class LHCoverageTest {

    @Rule
    public Timeout timeout = Timeout.seconds(5);

    private SUTForBookkeeper sut = SUTForBookkeeper.getInstance(3);
    private BookKeeper bkClient;
    private static final byte[] TEST_ENTRY = "some data".getBytes();
    private ClientConfiguration conf;
    @Before
    public void setUp() throws Exception {
        sut.setUp();
        conf = TestBKConfiguration.newClientConfiguration();
        conf.setMetadataServiceUri(sut.getZooKeeperCluster().getMetadataServiceUri());
        bkClient = new BookKeeper(conf);
    }

    @Test
    public void testAddEntryCloseWH(){

        LedgerHandle handle;
        try {
            handle = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());
            handle.close();
            handle.addEntry(TEST_ENTRY);
            fail();
        } catch (BKException e) {
            assertEquals(BKException.Code.LedgerClosedException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }


    }
    @Test
    public void testReadCtxClosed(){

        LedgerHandle handle;
        try {
            handle = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());

            handle.addEntry(TEST_ENTRY);
            handle.close();
            bkClient.close();
            handle.readEntries(0, 0);
            fail();
        } catch (BKException e) {
            assertEquals(BKException.Code.ClientClosedException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }

    }

    @Test
    public void testReadLastEntry(){
        LedgerHandle handle = null;
        try {
            handle = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());
            handle.readLastEntry();
            fail();
        } catch (BKException e) {
            assertEquals(BKException.Code.NoSuchEntryException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }

        try {

            Objects.requireNonNull(handle).addEntry(TEST_ENTRY);
            Objects.requireNonNull(handle).close();
            LedgerEntry entry = handle.readLastEntry();
            assertArrayEquals(TEST_ENTRY, entry.getEntry());
        } catch (InterruptedException | BKException e) {
            fail();
        }
    }

    @Test
    public void testReadNotConfirmed(){
        LedgerHandle handle = null;
        int num = 5;

        try {
            handle = bkClient.createLedger(3, 3,BookKeeper.DigestType.DUMMY, "password".getBytes());
            for (int i = 0; i < num; i++) {
                handle.addEntry(TEST_ENTRY);
            }

            Enumeration<LedgerEntry> entries = handle.readUnconfirmedEntries(0, num-1);
            while (entries.hasMoreElements()) {
                assertArrayEquals(TEST_ENTRY, entries.nextElement().getEntry());
            }
            entries = handle.readUnconfirmedEntries(num -2, num-1);
            while (entries.hasMoreElements()) {
                assertArrayEquals(TEST_ENTRY, entries.nextElement().getEntry());
            }
            handle.readUnconfirmedEntries(0, num);
            fail();

        } catch (BKException e) {
            assertEquals(BKException.Code.NoSuchEntryException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }

        try {
            assertNotNull(handle);
            handle.readUnconfirmedEntries(-1, num-1);
        } catch (BKException e) {
            assertEquals(BKException.Code.IncorrectParameterException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }

        try {
            assertNotNull(handle);
            handle.readUnconfirmedEntries(num, num -1);
        } catch (BKException e) {
            assertEquals(BKException.Code.IncorrectParameterException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testAddRejectedEntry(){
        try {
            LedgerHandle handle = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());
            LedgerHandle lhSpy = spy(handle);
            doThrow(new RejectedExecutionException()).when(lhSpy).executeOrdered(any());
            lhSpy.close();
            lhSpy.addEntry(TEST_ENTRY);
            fail();
        } catch (BKException e) {
            assertEquals(BKException.Code.InterruptedException, e.getCode());
        } catch (InterruptedException e) {
            fail();
        }

    }


    @Test
    public void waitForWriteTest(){
        conf.setWaitTimeoutOnBackpressureMillis(0);
        try {
            BookKeeper bkClient = new BookKeeper(conf);
            LedgerHandle handle = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());
            handle.addEntry(TEST_ENTRY);
            handle.addEntry(TEST_ENTRY);
            handle.close();
            assertArrayEquals(TEST_ENTRY, handle.readLastEntry().getEntry());
            Enumeration<LedgerEntry> entries = handle.readEntries(0, 1);
            while (entries.hasMoreElements()) {
                assertArrayEquals(TEST_ENTRY, entries.nextElement().getEntry());
            }

        } catch (IOException | BKException | InterruptedException e) {
            fail();
        }

    }

    @After
    public void tearDown() throws Exception {
        sut.tearDown();
    }
    @Test
    public void testIfLastUpdated(){
        try {
            LedgerHandle handle = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());

            handle.addEntry(TEST_ENTRY);
            handle.addEntry("test-entry2".getBytes());
            handle.readLastEntry().getEntry();
            handle.close();
            assertEquals(1L, handle.lastAddConfirmed);

        } catch (BKException | InterruptedException e) {
            fail();
        }


    }

    // Addedd after pit report
    @Test
    public void addMultipleEntry(){
        try {
            LedgerHandle handle = bkClient.createLedger(BookKeeper.DigestType.DUMMY, "password".getBytes());
            long id = handle.addEntry(TEST_ENTRY);
            assertEquals(0L, id);
            assertArrayEquals(TEST_ENTRY, handle.readLastEntry().getEntry());
            id = handle.addEntry("test-entry2".getBytes());
            assertEquals(1L, id);
            assertArrayEquals("test-entry2".getBytes(), handle.readLastEntry().getEntry());
            handle.close();
        } catch (BKException | InterruptedException e) {
            fail();
        }
    }

}
