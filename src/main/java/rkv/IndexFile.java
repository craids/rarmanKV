
package rkv;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Objects;

class IndexFile {

	private final int fileId;
	private final DBDirectory dbDirectory;
	private File backingFile;

	private FileChannel channel;

	private final RKVDBOptions options;

	private long unFlushedData = 0;

	static final String INDEX_FILE_NAME = ".index";
	private static final String nullMessage = "Index file entry cannot be null";

	IndexFile(int fileId, DBDirectory dbDirectory, RKVDBOptions options) {
		this.fileId = fileId;
		this.dbDirectory = dbDirectory;
		this.options = options;
	}

	void create() throws IOException {
		backingFile = getIndexFile();
		if (!backingFile.createNewFile()) {
			throw new IOException("Index file with id " + fileId + " already exists");
		}
		channel = new RandomAccessFile(backingFile, "rw").getChannel();
	}

	void createRepairFile() throws IOException {
		backingFile = getRepairFile();
		while (!backingFile.createNewFile()) {

			backingFile.delete();
		}
		channel = new RandomAccessFile(backingFile, "rw").getChannel();
	}

	void open() throws IOException {
		backingFile = getIndexFile();
		channel = new RandomAccessFile(backingFile, "rw").getChannel();
	}

	void close() throws IOException {
		if (channel != null) {
			channel.close();
		}
	}

	void delete() throws IOException {
		if (channel != null && channel.isOpen())
			channel.close();

		getIndexFile().delete();
	}

	void write(IndexFileEntry entry) throws IOException {
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

		unFlushedData += written;
		if (options.getFlushDataSizeBytes() != -1 && unFlushedData > options.getFlushDataSizeBytes()) {
			channel.force(false);
			unFlushedData = 0;
		}
	}

	void flushToDisk() throws IOException {
		if (channel != null && channel.isOpen())
			channel.force(true);
	}

	IndexFileIterator newIterator() throws IOException {
		return new IndexFileIterator();
	}

	Path getPath() {
		return backingFile.toPath();
	}

	private File getIndexFile() {
		return dbDirectory.getPath().resolve(fileId + INDEX_FILE_NAME).toFile();
	}

	private File getRepairFile() {
		return dbDirectory.getPath().resolve(fileId + INDEX_FILE_NAME + ".repair").toFile();
	}

	public class IndexFileIterator implements Iterator<IndexFileEntry> {

		private final ByteBuffer buffer;

		// TODO: index files are not that large, need to check the
		// performance since we are memory mapping it.
		public IndexFileIterator() throws IOException {
			buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
		}

		@Override
		public boolean hasNext() {
			return buffer.hasRemaining();
		}

		@Override
		public IndexFileEntry next() {
			if (hasNext()) {
				return IndexFileEntry.deserialize(buffer);
			}
			return null;
		}
	}
}
