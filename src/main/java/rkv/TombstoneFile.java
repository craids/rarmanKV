
package rkv;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

class TombstoneFile {

	private final File backingFile;
	private FileChannel channel;
	private final DBDirectory dbDirectory;

	private final RKVDBOptions options;

	private long unFlushedData = 0;
	private long writeOffset = 0;

	static final String TOMBSTONE_FILE_NAME = ".tombstone";
	private static final String nullMessage = "Tombstone entry cannot be null";

	static TombstoneFile create(DBDirectory dbDirectory, int fileId, RKVDBOptions options) throws IOException {
		File file = getTombstoneFile(dbDirectory, fileId);

		while (!file.createNewFile()) {
			// file already exists try another one.
			fileId++;
			file = getTombstoneFile(dbDirectory, fileId);
		}

		TombstoneFile tombstoneFile = new TombstoneFile(file, options, dbDirectory);
		tombstoneFile.open();

		return tombstoneFile;
	}

	TombstoneFile(File backingFile, RKVDBOptions options, DBDirectory dbDirectory) {
		this.backingFile = backingFile;
		this.options = options;
		this.dbDirectory = dbDirectory;
	}

	void open() throws IOException {
		channel = new RandomAccessFile(backingFile, "rw").getChannel();
	}

	void close() throws IOException {
		if (channel != null) {
			channel.close();
		}
	}

	void delete() throws IOException {
		close();
		if (backingFile != null) {
			backingFile.delete();
		}
	}

	void write(TombstoneEntry entry) throws IOException {
		Objects.requireNonNull(entry, nullMessage);

		ByteBuffer[] contents = entry.serialize();
		long toWrite = 0;
		for (ByteBuffer buffer : contents) {
			toWrite += buffer.remaining();
		}
		long written = 0;
		while (written < toWrite) {
			written += channel.write(contents);
		}

		writeOffset += written;
		unFlushedData += written;
		if (options.isSyncWrite()
				|| (options.getFlushDataSizeBytes() != -1 && unFlushedData > options.getFlushDataSizeBytes())) {
			flushToDisk();
			unFlushedData = 0;
		}
	}

	long getWriteOffset() {
		return writeOffset;
	}

	void flushToDisk() throws IOException {
		if (channel != null && channel.isOpen())
			channel.force(true);
	}

	TombstoneFile repairFile(DBDirectory dbDirectory) throws IOException {
		TombstoneFile repairFile = createRepairFile();

		TombstoneFileIterator iterator = newIteratorWithCheckForDataCorruption();
		int count = 0;
		while (iterator.hasNext()) {
			TombstoneEntry entry = iterator.next();
			if (entry == null) {

				break;
			}
			count++;
			repairFile.write(entry);
		}

		repairFile.flushToDisk();
		Files.move(repairFile.getPath(), getPath(), REPLACE_EXISTING, ATOMIC_MOVE);
		dbDirectory.syncMetaData();
		repairFile.close();
		close();
		open();
		return this;
	}

	private TombstoneFile createRepairFile() throws IOException {
		File repairFile = dbDirectory.getPath().resolve(getName() + ".repair").toFile();
		while (!repairFile.createNewFile()) {

			repairFile.delete();
		}

		TombstoneFile tombstoneFile = new TombstoneFile(repairFile, options, dbDirectory);
		tombstoneFile.open();
		return tombstoneFile;
	}

	String getName() {
		return backingFile.getName();
	}

	private Path getPath() {
		return backingFile.toPath();
	}

	private long getSize() {
		return backingFile.length();
	}

	TombstoneFile.TombstoneFileIterator newIterator() throws IOException {
		return new TombstoneFile.TombstoneFileIterator(false);
	}

	// Returns null when it finds a corrupted entry.
	TombstoneFile.TombstoneFileIterator newIteratorWithCheckForDataCorruption() throws IOException {
		return new TombstoneFile.TombstoneFileIterator(true);
	}

	private static File getTombstoneFile(DBDirectory dbDirectory, int fileId) {
		return dbDirectory.getPath().resolve(fileId + TOMBSTONE_FILE_NAME).toFile();
	}

	class TombstoneFileIterator implements Iterator<TombstoneEntry> {

		private final ByteBuffer buffer;
		private final boolean discardCorruptedRecords;

		TombstoneFileIterator(boolean discardCorruptedRecords) throws IOException {
			buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
			this.discardCorruptedRecords = discardCorruptedRecords;
		}

		@Override
		public boolean hasNext() {
			return buffer.hasRemaining();
		}

		@Override
		public TombstoneEntry next() {
			if (hasNext()) {
				if (discardCorruptedRecords)
					return TombstoneEntry.deserializeIfNotCorrupted(buffer);

				return TombstoneEntry.deserialize(buffer);
			}

			return null;
		}
	}
}
