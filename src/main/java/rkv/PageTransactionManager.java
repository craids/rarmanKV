
package rkv;

import javax.crypto.Cipher;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

// TODO: Handle the case where we are recovering lg9 and lg0, were we
// should start with lg9 instead of lg0!

final class PageTransactionManager {
	private PageFile owner;

	// streams for transaction log.
	private DataOutputStream oos;

	private ArrayList<PageIo> txn = new ArrayList<PageIo>();
	private int curTxn = -1;

	private Storage storage;
	private Cipher cipherIn;
	private Cipher cipherOut;

	PageTransactionManager(PageFile owner, Storage storage, Cipher cipherIn, Cipher cipherOut) throws IOException {
		this.owner = owner;
		this.storage = storage;
		this.cipherIn = cipherIn;
		this.cipherOut = cipherOut;
		recover();
		open();
	}

	public void synchronizeLog() throws IOException {
		synchronizeLogFromMemory();
	}

	private void synchronizeLogFromMemory() throws IOException {
		close();

		TreeSet<PageIo> pageList = new TreeSet<PageIo>(PAGE_IO_COMPARTOR);

		int numPages = 0;
		int writtenPages = 0;

		if (txn != null) {
			// Add each page to the pageList, replacing the old copy of this
			// page if necessary, thus avoiding writing the same page twice
			for (Iterator<PageIo> k = txn.iterator(); k.hasNext();) {
				PageIo page = k.next();
				if (pageList.contains(page)) {
					page.decrementTransactionCount();
				} else {
					writtenPages++;
					boolean result = pageList.add(page);
				}
				numPages++;
			}

			txn = null;
		}

		// Write the page from the pageList to disk
		synchronizePages(pageList, true);

		owner.sync();
		open();
	}

	private void open() throws IOException {

		oos = storage.openTransactionLog();
		oos.writeShort(Magic.LOGFILE_HEADER);
		oos.flush();
		curTxn = -1;
	}

	private void recover() throws IOException {

		DataInputStream ois = storage.readTransactionLog();

		// if transaction log is empty, or does not exist
		if (ois == null)
			return;

		while (true) {
			ArrayList<PageIo> pages = null;
			try {
				int size = LongPacker.unpackInt(ois);
				pages = new ArrayList<PageIo>(size);
				for (int i = 0; i < size; i++) {
					PageIo b = new PageIo();
					b.readExternal(ois, cipherOut);
					pages.add(b);
				}
			} catch (IOException e) {
				// corrupted logfile, ignore rest of transactions
				break;
			}
			synchronizePages(pages, false);

		}
		owner.sync();
		ois.close();
		storage.deleteTransactionLog();
	}

	private void synchronizePages(Iterable<PageIo> pages, boolean fromCore) throws IOException {
		// write pages vector elements to the data file.
		for (PageIo cur : pages) {
			owner.synch(cur);
			if (fromCore) {
				cur.decrementTransactionCount();
				if (!cur.isInTransaction()) {
					owner.releaseFromTransaction(cur);
				}
			}
		}
	}

	private void setClean(ArrayList<PageIo> pages) throws IOException {
		for (PageIo cur : pages) {
			cur.setClean();
		}
	}

	private void discardPages(ArrayList<PageIo> pages) throws IOException {
		for (PageIo cur : pages) {

			cur.decrementTransactionCount();
			if (!cur.isInTransaction()) {
				owner.releaseFromTransaction(cur);
			}
		}
	}

	void start() throws IOException {
		curTxn++;
		if (curTxn == 1) {
			synchronizeLogFromMemory();
			curTxn = 0;
		}
		txn = new ArrayList();
	}

	void add(PageIo page) throws IOException {
		page.incrementTransactionCount();
		txn.add(page);
	}

	void commit() throws IOException {
		LongPacker.packInt(oos, txn.size());
		for (PageIo page : txn) {
			page.writeExternal(oos, cipherIn);
		}

		sync();

		// set clean flag to indicate pages have been written to log
		setClean(txn);

		// open a new ObjectOutputStream in order to store
		// newer states of PageIo
//        oos = new DataOutputStream(new BufferedOutputStream(fos));
	}

	private void sync() throws IOException {
		oos.flush();
	}

	void shutdown() throws IOException {
		synchronizeLogFromMemory();
		close();
	}

	private void close() throws IOException {
		sync();
		oos.close();
		oos = null;
	}

	void forceClose() throws IOException {
		oos.close();
		oos = null;
	}

	void synchronizeLogFromDisk() throws IOException {
		close();

		if (txn != null) {
			discardPages(txn);
			txn = null;
		}

		recover();
		open();
	}

	private static final Comparator<PageIo> PAGE_IO_COMPARTOR = new Comparator<PageIo>() {

		public int compare(PageIo page1, PageIo page2) {

			if (page1.getPageId() == page2.getPageId()) {
				return 0;
			} else if (page1.getPageId() < page2.getPageId()) {
				return -1;
			} else {
				return 1;
			}
		}

	};

}
