package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Mockito.*;

public class AsyncLLMCreateLedgerTest extends AbsBkCreate {

    public AsyncLLMCreateLedgerTest() {
        super(3, 3, 2, DigestType.CRC32, "pass".getBytes(), null, TestOutcome.VALID);
    }

    @Test
    public void testValidLedgerCreation() throws Exception {
        CreateCallback cb = mock(CreateCallback.class);

        bkClient.asyncCreateLedger(3, 3, 2, DigestType.CRC32, "pwd".getBytes(), cb, null, null);
        Thread.sleep(500);

        verify(cb, timeout(1000).times(1)).createComplete(eq(BKException.Code.OK), notNull(), eq(null));
    }

    @Test
    public void testAckGreaterThanWriteQuorum() throws Exception {
        CreateCallback cb = mock(CreateCallback.class);

        try {
            bkClient.asyncCreateLedger(3, 2, 3, DigestType.CRC32, "pwd".getBytes(), cb, null, null);
            Thread.sleep(500);
        } catch (IllegalArgumentException e) {
            // expected
            return;
        }

        ArgumentCaptor<Integer> rcCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(cb, timeout(1000).times(1)).createComplete(rcCaptor.capture(), any(), eq(null));
        assertNotEquals("Ack quorum > Write quorum should fail", BKException.Code.OK, rcCaptor.getValue().intValue());
    }

    @Test
    public void testNullDigest() throws Exception {
        CreateCallback cb = mock(CreateCallback.class);

        try {
            bkClient.asyncCreateLedger(3, 3, 2, null, "pwd".getBytes(), cb, null, null);
            Thread.sleep(500);
        } catch (NullPointerException | IllegalArgumentException e) {
            // expected
            return;
        }

        verify(cb, timeout(1000).times(1)).createComplete(not(eq(BKException.Code.OK)), isNull(), eq(null));
    }

    @Test
    public void testNullPassword() throws Exception {
        CreateCallback cb = mock(CreateCallback.class);

        try {
            bkClient.asyncCreateLedger(3, 3, 2, DigestType.CRC32, null, cb, null, null);
            Thread.sleep(500);
        } catch (NullPointerException | IllegalArgumentException e) {
            // expected
            return;
        }

        verify(cb, timeout(1000).times(1)).createComplete(not(eq(BKException.Code.OK)), isNull(), eq(null));
    }


    @Test
    public void testWithCustomMetadata() throws Exception {
        CreateCallback cb = mock(CreateCallback.class);
        Map<String, byte[]> metadata = new HashMap<>();
        metadata.put("test", ("value".getBytes()));

        bkClient.asyncCreateLedger(3, 3, 2, DigestType.MAC, "pwd".getBytes(), cb, null, metadata);
        Thread.sleep(500);

        verify(cb, timeout(1000).times(1)).createComplete(eq(BKException.Code.OK), notNull(), eq(null));
    }

    @Test
    public void testLedgerHandleIsCorrect() throws Exception {
        CreateCallback cb = mock(CreateCallback.class);
        ArgumentCaptor<LedgerHandle> handleCaptor = ArgumentCaptor.forClass(LedgerHandle.class);

        bkClient.asyncCreateLedger(3, 3, 2, DigestType.CRC32, "pwd".getBytes(), cb, null, null);
        Thread.sleep(500);

        verify(cb, timeout(1000).times(1)).createComplete(eq(BKException.Code.OK), handleCaptor.capture(), eq(null));
        LedgerHandle lh = handleCaptor.getValue();
        assertNotNull("LedgerHandle should be created", lh);
        assertTrue("Ledger ID should be positive", lh.getId() >= 0);
    }
}
