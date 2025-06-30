package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.util.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.TmpDirs;

import java.io.File;

import static org.apache.bookkeeper.bookie.ApacheBookieJournalUtil.writeV4Journal;
import static org.apache.bookkeeper.bookie.ApacheBookieJournalUtil.writeV5Journal;

public class JournalSUT {

    private BookieImpl bookie;
    private TmpDirs tmpDirs;
    public static final int NUM_OF_ENTRIES_ON_JOURNAL = 200;
    public static final String JOURNAL_TEST_V4 = "JOURNAL-TEST-V4";
    public static final String JOURNAL_TEST_V5 = "JOURNAL-TEST-V5";

    public void shutdownBookie() {
        bookie.shutdown();
    }

    public void cleanupDirs() throws Exception {
        tmpDirs.cleanup();
    }

    public Journal createJournal() throws Exception {
        File journalDir = tmpDirs.createNew(JOURNAL_TEST_V4, "journal");
        File ledgerDir = tmpDirs.createNew(JOURNAL_TEST_V4, "ledger");

        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        JournalChannel journalChannel = writeV4Journal(BookieImpl.getCurrentDirectory(journalDir),
                NUM_OF_ENTRIES_ON_JOURNAL, "test".getBytes());
        JournalTest.WRITTEN_BYTES = journalChannel.fc.position();


        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf
                .setJournalDirsName(new String[] {journalDir.getPath()})
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setMetadataServiceUri(null);

        bookie = new TestBookieImpl(conf);
        return bookie.journals.get(0);

    }

    public Journal createJournalV5() throws Exception {

        File journalDir = tmpDirs.createNew(JOURNAL_TEST_V5, "journal");
        File ledgerDir = tmpDirs.createNew(JOURNAL_TEST_V5, "ledger");

        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDir));
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDir));

        JournalChannel jc = writeV5Journal(BookieImpl.getCurrentDirectory(journalDir), NUM_OF_ENTRIES_ON_JOURNAL, "test".getBytes());
        JournalV5Test.WRITTEN_BYTES_V5 = jc.fc.position();


        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf
                .setJournalDirsName(new String[] {journalDir.getPath()})
                .setLedgerDirNames(new String[] { ledgerDir.getPath() })
                .setMetadataServiceUri(null);

        bookie = new TestBookieImpl(conf);
        return bookie.journals.get(0);

    }

}
