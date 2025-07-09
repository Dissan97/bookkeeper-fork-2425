package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.TestOutcome;
import org.apache.bookkeeper.conf.TestConfig;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;


public class LedgerHandleIT extends BookKeeperClusterTestCase {
    private static final byte[] LEDGER_PASSWORD = "password".getBytes();
    ClientConfiguration conf;
    AtomicLong lhId = new AtomicLong(0);
    public static final List<byte[]> ENTRIES = IntStream.range(0, 10)
            .mapToObj(i -> ("entry" + i).getBytes(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
    private static final int ENS_SIZE = 3;
    private BookKeeper bkClient;

    private BookKeeper client;
    private LedgerHandle handle;

    public LedgerHandleIT() {
        super(ENS_SIZE);
    }

    @Before
    public void createLedger() throws BKException, IOException, InterruptedException {

        client = new BookKeeper(super.baseClientConf, super.zkc);
        conf = super.baseClientConf;
    }

    @Test
    public void readSameLedger() throws Exception {
        BookKeeper dummy = new BookKeeper(conf);
        LedgerHandle lhDummy = dummy.createLedger(ENS_SIZE, ENS_SIZE,ENS_SIZE,
                BookKeeper.DigestType.DUMMY, LEDGER_PASSWORD);
        lhId.set(lhDummy.getId());

        for (int i = 0; i < ENTRIES.size(); i++) {
            lhDummy.addEntry(ENTRIES.get(i));
        }
        lhDummy.close();
        dummy.close();
        boolean[] passed = new boolean[]{false, false, false, false};
        Thread[] threads = new Thread[passed.length];
        CountDownLatch latch = new CountDownLatch(passed.length);
        for (int i = 0; i < passed.length; i++) {
            final int id = i;
            threads[i] = new Thread(() -> {
                try (BookKeeper bookKeeper = new BookKeeper(conf, super.zkc)){

                    LedgerHandle lh = bookKeeper.openLedger(lhId.get(), BookKeeper.DigestType.DUMMY, LEDGER_PASSWORD);
                    Enumeration<LedgerEntry> entries = lh.readEntries(0, ENTRIES.size()-1);
                    for (byte[] entry : ENTRIES) {
                        passed[id] = Arrays.equals(entry, entries.nextElement().getEntry());
                    }
                } catch (IOException | InterruptedException | BKException e) {
                    passed[id] = false;
                }finally {
                    latch.countDown();
                }
            });
        }
        for (int i = 0; i < passed.length; i++) {
            threads[i].start();
        }
        latch.await();
        for (boolean pass : passed) {
            assertTrue(pass);
        }

    }

    @Test
    public void readSameOneBookieFailure() throws Exception {
        BookKeeper dummy = new BookKeeper(conf);
        LedgerHandle lhDummy = dummy.createLedger(ENS_SIZE, ENS_SIZE-1,ENS_SIZE-1,
                BookKeeper.DigestType.CRC32, LEDGER_PASSWORD);
        lhId.set(lhDummy.getId());
        for (int i = 0; i < ENTRIES.size(); i++) {

            lhDummy.addEntry(ENTRIES.get(i));
        }
        lhDummy.close();
        dummy.close();

        super.killBookie(0);



        boolean[] passed = new boolean[]{false, false, false, false};
        Thread[] threads = new Thread[passed.length];
        CountDownLatch latch = new CountDownLatch(passed.length);
        for (int i = 0; i < passed.length; i++) {
            final int id = i;
            threads[i] = new Thread(() -> {
                try (BookKeeper bookKeeper = new BookKeeper(conf, super.zkc)){

                    LedgerHandle lh = bookKeeper.openLedger(lhId.get(), BookKeeper.DigestType.CRC32, LEDGER_PASSWORD);
                    Enumeration<LedgerEntry> entries = lh.readEntries(0, ENTRIES.size()-1);
                    for (byte[] entry : ENTRIES) {
                        passed[id] = Arrays.equals(entry, entries.nextElement().getEntry());
                    }
                } catch (IOException | InterruptedException | BKException e) {
                    passed[id] = false;
                }finally {
                    latch.countDown();
                }
            });
        }
        for (int i = 0; i < passed.length; i++) {
            threads[i].start();
        }
        latch.await();
        for (boolean pass : passed) {
            assertTrue(pass);
        }

    }

    @Test
    public void testWriteFailsWhenBookieIsUnavailable() throws Exception {
        // Create ledger with ensemble of 3, write quorum = 3, ack quorum = 2
        BookKeeper bk = new BookKeeper(conf);
        LedgerHandle lh = bk.createLedger(3, 3, 2,
                BookKeeper.DigestType.CRC32, LEDGER_PASSWORD);

        // Kill one Bookie (e.g., Bookie 0)
        super.killBookie(0);

        // Try writing, expect BKException with specific code
        try {
            lh.addEntry("fail-entry".getBytes(StandardCharsets.UTF_8));
            // If no exception is thrown, this is an error
            fail("Expected BKException was not thrown");
        } catch (BKException e) {
            // Check that the exception is the expected type
            assertEquals("Expected BookieHandleNotAvailableException",
                    BKException.Code.NotEnoughBookiesException, e.getCode());
        } catch (InterruptedException e) {
            fail("Test interrupted unexpectedly");
        } finally {
            lh.close();
            bk.close();
        }

    }

}
