
package rkv;

import java.io.IOException;
import java.util.Arrays;

final class LogicalRowIdManager {
	// our record file and associated page manager
	private final PageFile file;
	private final PageManager pageman;
	static final short ELEMS_PER_PAGE = (short) ((Storage.PAGE_SIZE - Magic.PAGE_HEADER_SIZE)
			/ Magic.PhysicalRowId_SIZE);

	private long[] freeRecordsInTransRowid = new long[4];
	private int freeRecordsInTransSize = 0;

	static final int OFFSET_FREE_COUNT = Magic.PAGE_HEADER_SIZE;
	static final int FREE_HEADER_SIZE = Magic.PAGE_HEADER_SIZE + Magic.SZ_SHORT;

	static final int FREE_RECORDS_PER_PAGE = (Storage.PAGE_SIZE - FREE_HEADER_SIZE) / 6;

	LogicalRowIdManager(PageFile file, PageManager pageman) throws IOException {
		this.file = file;
		this.pageman = pageman;
	}

	long insert(final long physloc) throws IOException {
		// check whether there's a free rowid to reuse
		long retval = getFreeSlot();
		if (retval == 0) {
			// no. This means that we bootstrap things by allocating
			// a new translation page and freeing all the rowids on it.
			long firstPage = pageman.allocate(Magic.TRANSLATION_PAGE);
			short curOffset = Magic.PAGE_HEADER_SIZE;
			for (int i = 0; i < ELEMS_PER_PAGE; i++) {
				putFreeSlot(((-firstPage) << Storage.PAGE_SIZE_SHIFT) + (long) curOffset);

				curOffset += Magic.PhysicalRowId_SIZE;
			}

			retval = getFreeSlot();
			if (retval == 0) {
				throw new Error("couldn't obtain free translation");
			}
		}
		// write the translation.
		update(retval, physloc);
		return retval;
	}

	void forceInsert(final long logicalRowId, final long physLoc) throws IOException {
		if (fetch(logicalRowId) != 0)
			throw new Error("can not forceInsert, record already exists: " + logicalRowId);

		update(logicalRowId, physLoc);
	}

	void delete(final long logicalrowid) throws IOException {
		// zero out old location, is needed for defragmentation
		final long pageId = -(logicalrowid >>> Storage.PAGE_SIZE_SHIFT);
		final PageIo xlatPage = file.get(pageId);
		xlatPage.pageHeaderSetLocation((short) (logicalrowid & Storage.OFFSET_MASK), 0);
		file.release(pageId, true);
		putFreeSlot(logicalrowid);
	}

	void update(final long logicalrowid, final long physloc) throws IOException {

		final long pageId = -(logicalrowid >>> Storage.PAGE_SIZE_SHIFT);
		final PageIo xlatPage = file.get(pageId);
		xlatPage.pageHeaderSetLocation((short) (logicalrowid & Storage.OFFSET_MASK), physloc);
		file.release(pageId, true);
	}

	long fetch(long logicalrowid) throws IOException {
		final long pageId = -(logicalrowid >>> Storage.PAGE_SIZE_SHIFT);
		final long last = pageman.getLast(Magic.TRANSLATION_PAGE);
		if (last - 1 > pageId)
			return 0;

		final short offset = (short) (logicalrowid & Storage.OFFSET_MASK);

		final PageIo xlatPage = file.get(pageId);
		final long ret = xlatPage.pageHeaderGetLocation(offset);

		file.release(pageId, false);
		return ret;
	}

	void commit() throws IOException {
		if (freeRecordsInTransSize == 0)
			return;

		long freeRecPageId = pageman.getLast(Magic.FREELOGIDS_PAGE);
		if (freeRecPageId == 0) {
			// allocate new
			freeRecPageId = pageman.allocate(Magic.FREELOGIDS_PAGE);
		}
		PageIo freeRecPage = file.get(freeRecPageId);
		// write all uncommited free records
		for (int rowPos = 0; rowPos < freeRecordsInTransSize; rowPos++) {
			short count = freeRecPage.readShort(OFFSET_FREE_COUNT);
			if (count == FREE_RECORDS_PER_PAGE) {
				// allocate new free recid page
				file.release(freeRecPage);
				freeRecPageId = pageman.allocate(Magic.FREELOGIDS_PAGE);
				freeRecPage = file.get(freeRecPageId);
				freeRecPage.writeShort(FREE_RECORDS_PER_PAGE, (short) 0);
				count = 0;
			}
			final int offset = (count) * 6 + FREE_HEADER_SIZE;
			// write free recid and increase counter
			freeRecPage.writeSixByteLong(offset, freeRecordsInTransRowid[rowPos]);
			count++;
			freeRecPage.writeShort(OFFSET_FREE_COUNT, count);

		}
		file.release(freeRecPage);

		clearFreeRecidsInTransaction();
	}

	private void clearFreeRecidsInTransaction() {
		if (freeRecordsInTransRowid.length > 128)
			freeRecordsInTransRowid = new long[4];
		freeRecordsInTransSize = 0;
	}

	void rollback() throws IOException {
		clearFreeRecidsInTransaction();
	}

	long getFreeSlot() throws IOException {
		if (freeRecordsInTransSize != 0) {
			return freeRecordsInTransRowid[--freeRecordsInTransSize];
		}

		final long logicFreePageId = pageman.getLast(Magic.FREELOGIDS_PAGE);
		if (logicFreePageId == 0) {
			return 0;
		}
		PageIo logicFreePage = file.get(logicFreePageId);
		short recCount = logicFreePage.readShort(OFFSET_FREE_COUNT);
		if (recCount <= 0) {
			throw new InternalError();
		}

		final int offset = (recCount - 1) * 6 + FREE_HEADER_SIZE;
		final long ret = logicFreePage.readSixByteLong(offset);

		recCount--;

		if (recCount > 0) {
			// decrease counter and zero out old record
			logicFreePage.writeSixByteLong(offset, 0);
			logicFreePage.writeShort(OFFSET_FREE_COUNT, recCount);
			file.release(logicFreePage);
		} else {
			// release this page
			file.release(logicFreePage);
			pageman.free(Magic.FREELOGIDS_PAGE, logicFreePageId);
		}

		return ret;
	}

	void putFreeSlot(long rowid) throws IOException {
		// ensure capacity
		if (freeRecordsInTransSize == freeRecordsInTransRowid.length)
			freeRecordsInTransRowid = Arrays.copyOf(freeRecordsInTransRowid, freeRecordsInTransRowid.length * 4);
		// add record and increase size
		freeRecordsInTransRowid[freeRecordsInTransSize] = rowid;
		freeRecordsInTransSize++;
	}

}
