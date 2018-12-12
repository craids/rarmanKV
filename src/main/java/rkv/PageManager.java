
package rkv;

import java.io.IOException;
import java.nio.ByteBuffer;

final class PageManager {
	// our record file
	final PageFile file;

	private PageIo headerBuf;

	PageManager(PageFile file) throws IOException {
		this.file = file;

		// check the file headerBuf.fileHeader If the magic is 0, we assume a new
		// file. Note that we hold on to the file header node.
		headerBuf = file.get(0);
		headerBuf.ensureHeapBuffer();
		headerBuf.fileHeaderCheckHead(headerBuf.readShort(0) == 0);
	}

	long allocate(short type) throws IOException {

		if (type == Magic.FREE_PAGE)
			throw new Error("allocate of free page?");

		// do we have something on the free list?
		long retval = headerBuf.fileHeaderGetFirstOf(Magic.FREE_PAGE);
		boolean isNew = false;

		if (type != Magic.TRANSLATION_PAGE) {

			if (retval != 0) {
				// yes. Point to it and make the next of that page the
				// new first free page.
				headerBuf.fileHeaderSetFirstOf(Magic.FREE_PAGE, getNext(retval));
			} else {
				// nope. make a new record
				retval = headerBuf.fileHeaderGetLastOf(Magic.FREE_PAGE);
				if (retval == 0)
					// very new file - allocate record #1
					retval = 1;
				headerBuf.fileHeaderSetLastOf(Magic.FREE_PAGE, retval + 1);
				isNew = true;
			}
		} else {
			// translation pages have different allocation scheme
			// and also have negative address
			retval = headerBuf.fileHeaderGetLastOf(Magic.TRANSLATION_PAGE) - 1;
			isNew = true;
		}

		// Cool. We have a record, add it to the correct list
		PageIo pageHdr = file.get(retval);
		if (isNew) {
			pageHdr.pageHeaderSetType(type);
		} else {
			if (!pageHdr.pageHeaderMagicOk())
				throw new Error("CRITICAL: page header magic for page " + pageHdr.getPageId() + " not OK "
						+ pageHdr.pageHeaderGetMagic());
		}
		long oldLast = headerBuf.fileHeaderGetLastOf(type);

		// Clean data.
		pageHdr.writeByteArray(PageFile.CLEAN_DATA, 0, 0, Storage.PAGE_SIZE);

		pageHdr.pageHeaderSetType(type);
		pageHdr.pageHeaderSetPrev(oldLast);
		pageHdr.pageHeaderSetNext(0);

		if (oldLast == 0)
			// This was the first one of this type
			headerBuf.fileHeaderSetFirstOf(type, retval);
		headerBuf.fileHeaderSetLastOf(type, retval);
		file.release(retval, true);

		// If there's a previous, fix up its pointer
		if (oldLast != 0) {
			pageHdr = file.get(oldLast);
			pageHdr.pageHeaderSetNext(retval);
			file.release(oldLast, true);
		}

		return retval;
	}

	void free(short type, long recid) throws IOException {
		if (type == Magic.FREE_PAGE)
			throw new Error("free free page?");
		if (type == Magic.TRANSLATION_PAGE)
			throw new Error("Translation page can not be dealocated");

		if (recid == 0)
			throw new Error("free header page?");

		// get the page and read next and previous pointers
		PageIo pageHdr = file.get(recid);
		long prev = pageHdr.pageHeaderGetPrev();
		long next = pageHdr.pageHeaderGetNext();

		// put the page at the front of the free list.
		pageHdr.pageHeaderSetType(Magic.FREE_PAGE);
		pageHdr.pageHeaderSetNext(headerBuf.fileHeaderGetFirstOf(Magic.FREE_PAGE));
		pageHdr.pageHeaderSetPrev(0);

		headerBuf.fileHeaderSetFirstOf(Magic.FREE_PAGE, recid);
		file.release(recid, true);

		// remove the page from its old list
		if (prev != 0) {
			pageHdr = file.get(prev);
			pageHdr.pageHeaderSetNext(next);
			file.release(prev, true);
		} else {
			headerBuf.fileHeaderSetFirstOf(type, next);
		}
		if (next != 0) {
			pageHdr = file.get(next);
			pageHdr.pageHeaderSetPrev(prev);
			file.release(next, true);
		} else {
			headerBuf.fileHeaderSetLastOf(type, prev);
		}

	}

	long getNext(long page) throws IOException {
		try {
			return file.get(page).pageHeaderGetNext();
		} finally {
			file.release(page, false);
		}
	}

	long getPrev(long page) throws IOException {
		try {
			return file.get(page).pageHeaderGetPrev();
		} finally {
			file.release(page, false);
		}
	}

	long getFirst(short type) throws IOException {
		return headerBuf.fileHeaderGetFirstOf(type);
	}

	long getLast(short type) throws IOException {
		return headerBuf.fileHeaderGetLastOf(type);
	}

	void commit() throws IOException {
		// write the header out
		file.release(headerBuf);
		file.commit();

		// and obtain it again
		headerBuf = file.get(0);
		headerBuf.ensureHeapBuffer();
		headerBuf.fileHeaderCheckHead(headerBuf.readShort(0) == 0);
	}

	void rollback() throws IOException {
		// release header
		file.discard(headerBuf);
		file.rollback();
		// and obtain it again
		headerBuf = file.get(0);
		headerBuf.ensureHeapBuffer();
		headerBuf.fileHeaderCheckHead(headerBuf.readShort(0) == 0);
	}

	void close() throws IOException {
		file.release(headerBuf);
		file.commit();
		headerBuf = null;
	}

	ByteBuffer getHeaderBufData() {
		return headerBuf.getData();
	}

	public PageIo getFileHeader() {
		return headerBuf;
	}
}
