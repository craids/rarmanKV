
package rkv;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class RKVDBIterator implements Iterator<Record> {

	private Iterator<Integer> outer;
	private Iterator<IndexFileEntry> inner;
	private RKVDBFile currentFile;

	private Record next;

	private final RKVDBInternal dbInternal;

	RKVDBIterator(RKVDBInternal dbInternal) {
		this.dbInternal = dbInternal;
		outer = dbInternal.listDataFileIds().iterator();
	}

	@Override
	public boolean hasNext() {
		if (next != null) {
			return true;
		}

		try {
			// inner == null means this is the first time hasNext() is called.
			// use moveToNextFile() to move to the first file.
			if (inner == null && !moveToNextFile()) {
				return false;
			}

			do {
				if (readNextRecord()) {
					return true;
				}
			} while (moveToNextFile());

			return false;

		} catch (IOException e) {

			return false;
		}
	}

	@Override
	public Record next() {
		if (hasNext()) {
			Record record = next;
			next = null;
			return record;
		}
		throw new NoSuchElementException();
	}

	private boolean moveToNextFile() throws IOException {
		while (outer.hasNext()) {
			int fileId = outer.next();
			currentFile = dbInternal.getHaloDBFile(fileId);
			if (currentFile != null) {
				try {
					inner = currentFile.getIndexFile().newIterator();
					return true;
				} catch (ClosedChannelException e) {
					if (dbInternal.isClosing()) {
						// TODO: define custom Exception classes for HaloDB.
						throw new RuntimeException("DB is closing");
					}

				}
			}

		}

		return false;
	}

	private boolean readNextRecord() {
		while (inner.hasNext()) {
			IndexFileEntry entry = inner.next();
			try {
				try {
					next = readRecordFromDataFile(entry);
					if (next != null) {
						return true;
					}
				} catch (ClosedChannelException e) {
					if (dbInternal.isClosing()) {
						throw new RuntimeException("DB is closing");
					}

					break;
				}
			} catch (IOException e) {

				break;
			}
		}
		return false;
	}

	private Record readRecordFromDataFile(IndexFileEntry entry) throws IOException {
		InMemoryIndexMetaData meta = Utils.getMetaData(entry, currentFile.getFileId());
		Record record = null;
		if (dbInternal.isRecordFresh(entry.getKey(), meta)) {
			byte[] value = currentFile.readFromFile(Utils.getValueOffset(entry.getRecordOffset(), entry.getKey()),
					Utils.getValueSize(entry.getRecordSize(), entry.getKey()));
			record = new Record(entry.getKey(), value);
			record.setRecordMetaData(meta);
		}
		return record;
	}
}
