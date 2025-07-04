package org.apache.bookkeeper.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

public class LHLLMTest extends LedgerHandleSUT {

    @Test
    public void testInvalidConstants() {
        // Verifica delle costanti statiche
        assertEquals("INVALID_ENTRY_ID deve corrispondere al protocollo",
                BookieProtocol.INVALID_ENTRY_ID, LedgerHandle.INVALID_ENTRY_ID);
        assertEquals("INVALID_LEDGER_ID deve essere -0xABCDABCDL",
                -0xABCDABCDL, LedgerHandle.INVALID_LEDGER_ID);
    }

    @Test
    public void testSetLastAddConfirmedAndIsHandleWritable() throws Exception {
        // Initially writable
        assertTrue("Handle scrivibile prima di close()", lh.isHandleWritable());

        // Modifico lastAddConfirmed via package-private setter
        lh.setLastAddConfirmed(123L);
        assertEquals("getLastAddConfirmed() deve riflettere il valore settato",
                123L, lh.getLastAddConfirmed());

        // Dopo close() non è più scrivibile
        lh.close();
        assertFalse("Handle non scrivibile dopo close()", lh.isHandleWritable());
    }

    @Test
    public void testGetVersionedLedgerMetadata() {
        // Versioned metadata deve essere non-null e coerente con getLedgerMetadata()
        Versioned versioned = lh.getVersionedLedgerMetadata();
        assertNotNull("Versioned metadata non deve essere null", versioned);
        assertSame("Il valore della versione deve corrispondere",
                versioned.getValue(), lh.getLedgerMetadata());
    }

    @Test
    public void testAppendByteBufferAndReadBack() throws Exception {
        // Uso dell'API nuova (WriteHandle.append(ByteBuffer))
        byte[] payload = "buffer-test".getBytes();
        long entryId = ((WriteHandle) lh).append(ByteBuffer.wrap(payload));

        assertEquals("Il primo entryId deve essere 0", 0L, entryId);
        assertEquals("lastAddPushed deve essere aggiornato", entryId, lh.getLastAddPushed());

        // Leggo l'entry e verifico il contenuto
        Enumeration<LedgerEntry> entries = lh.readEntries(entryId, entryId);
        LedgerEntry e = entries.nextElement();
        assertEquals("buffer-test", new String(e.getEntry()));
    }

    @Test
    public void testAppendAsync() throws Exception {
        byte[] payload = "async-test".getBytes();
        CompletableFuture<Long> f = ((WriteHandle) lh).appendAsync(Unpooled.wrappedBuffer(payload));
        long id = f.get();
        assertEquals("appendAsync deve restituire 0 per il primo entry", 0L, id);
        assertEquals("lastAddPushed deve essere sincronizzato", id, lh.getLastAddPushed());
    }

    @Test
    public void testGetWriteFlags() throws Exception {
        // Creo un nuovo ledger con WriteFlag.DEFERRED_SYNC
        EnumSet<WriteFlag> flags = EnumSet.of(WriteFlag.DEFERRED_SYNC);
        WriteHandle lh2 = bkClient.newCreateLedgerOp()
                .withDigestType(DigestType.DUMMY)
                .withPassword("pwd".getBytes())
                .withWriteFlags(flags)
                .execute()
                .get();
        assertTrue("writeFlags deve contenere il flag specificato",
                ((LedgerHandle) lh2).getWriteFlags().contains(WriteFlag.DEFERRED_SYNC));
        lh2.close();
    }

    @Test
    public void testAddToLengthAndGetLength() {
        long initial = lh.getLength();
        long updated = lh.addToLength(500);
        assertEquals("addToLength somma correttamente", initial + 500, updated);
        assertEquals("getLength riflette il nuovo valore", updated, lh.getLength());
    }
    @Test
    public void testReadEntriesRange() throws Exception {
        // Scrivo 5 entry: "val0" .. "val4"
        for (int i = 0; i < 5; i++) {
            lh.addEntry(("val" + i).getBytes());
        }

        // Leggo solo l’intervallo [1..3]
        Enumeration<LedgerEntry> entries = lh.readEntries(1, 3);
        List<String> values = new ArrayList<>();
        while (entries.hasMoreElements()) {
            values.add(new String(entries.nextElement().getEntry()));
        }

        assertEquals(
                "readEntries(1,3) deve restituire esattamente 3 valori",
                Arrays.asList("val1", "val2", "val3"),
                values
        );
    }

    @Test
    public void testBatchReadEntriesCountLimit() throws Exception {
        // Scrivo 5 entry: "v0" .. "v4"
        for (int i = 0; i < 5; i++) {
            lh.addEntry(("v" + i).getBytes());
        }

        // batchReadEntries con maxCount = 2
        Enumeration<LedgerEntry> entries = lh.batchReadEntries(0, 2, Long.MAX_VALUE);
        List<String> values = new ArrayList<>();
        while (entries.hasMoreElements()) {
            values.add(new String(entries.nextElement().getEntry()));
        }

        assertEquals("Devono tornare solo 2 entry", 2, values.size());
        assertEquals("v0", values.get(0));
        assertEquals("v1", values.get(1));
    }

    @Test(expected = BKException.BKReadException.class)
    public void testBatchReadEntriesInvalidStart() throws Exception {
        // startEntry > lastEntry disponibile; deve sollevare BKReadException
        lh.batchReadEntries(10, 5, Long.MAX_VALUE).nextElement();
    }

    @Test
    public void testBatchReadUnconfirmedEntries() throws Exception {
        // Scrivo 3 entry non confermate (ledger aperto)
        for (int i = 0; i < 3; i++) {
            lh.addEntry(("u" + i).getBytes());
        }

        Enumeration<LedgerEntry> entries =
                lh.batchReadUnconfirmedEntries(0, 3, Long.MAX_VALUE);
        List<String> values = new ArrayList<>();
        while (entries.hasMoreElements()) {
            values.add(new String(entries.nextElement().getEntry()));
        }

        assertEquals(
                "batchReadUnconfirmedEntries con intervallo completo",
                Arrays.asList("u0", "u1", "u2"),
                values
        );
    }

    @Test(expected = BKException.BKNoSuchEntryException.class)
    public void testBatchReadUnconfirmedEntriesInvalidRange() throws Exception {
        // Intervallo fuori range deve sollevare BKReadException
        lh.addEntry(("u" + 0).getBytes());
        lh.close();
        lh = bkClient.openLedger(0, BookKeeper.DigestType.DUMMY, "password".getBytes());
        lh.batchReadUnconfirmedEntries(5, 1, Long.MAX_VALUE).nextElement();
    }
}
