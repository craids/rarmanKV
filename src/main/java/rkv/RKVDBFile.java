
package rkv;

import com.google.common.primitives.Ints;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.regex.Matcher;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

class RKVDBFile {

	private volatile int writeOffset;

	private FileChannel channel;
	private final File backingFile;
	private final DBDirectory dbDirectory;
	private final int fileId;

	private IndexFile indexFile;

	private final RKVDBOptions options;

	private long unFlushedData = 0;

	static final String DATA_FILE_NAME = ".data";
	static final String COMPACTED_DATA_FILE_NAME = ".datac";

	private final FileType fileType;

	private RKVDBFile(int fileId, File backingFile, DBDirectory dbDirectory, IndexFile indexFile, FileType fileType,
			FileChannel channel, RKVDBOptions options) throws IOException {
		this.fileId = fileId;
		this.backingFile = backingFile;
		this.dbDirectory = dbDirectory;
		this.indexFile = indexFile;
		this.fileType = fileType;
		this.channel = channel;
		this.writeOffset = Ints.checkedCast(channel.size());
		this.options = options;
	}

	byte[] readFromFile(int offset, int length) throws IOException {
		byte[] value = new byte[length];
		ByteBuffer valueBuf = ByteBuffer.wrap(value);
		int read = readFromFile(offset, valueBuf);
		assert read == length;

		return value;
	}

	int readFromFile(long position, ByteBuffer destinationBuffer) throws IOException {
		long currentPosition = position;
		int bytesRead;
		do {
			bytesRead = channel.read(destinationBuffer, currentPosition);
			currentPosition += bytesRead;
		} while (bytesRead != -1 && destinationBuffer.hasRemaining());

		return (int) (currentPosition - position);
	}

	private Record readRecord(int offset) throws RKVDBException, IOException {
		long tempOffset = offset;

		// read the header from disk.
		ByteBuffer headerBuf = ByteBuffer.allocate(Record.Header.HEADER_SIZE);
		int readSize = readFromFile(offset, headerBuf);
		if (readSize != Record.Header.HEADER_SIZE) {
			throw new RKVDBException("Corrupted header at " + offset + " in file " + fileId);
		}
		tempOffset += readSize;

		Record.Header header = Record.Header.deserialize(headerBuf);
		if (!Record.Header.verifyHeader(header)) {
			throw new RKVDBException("Corrupted header at " + offset + " in file " + fileId);
		}

		// read key-value from disk.
		ByteBuffer recordBuf = ByteBuffer.allocate(header.getKeySize() + header.getValueSize());
		readSize = readFromFile(tempOffset, recordBuf);
		if (readSize != recordBuf.capacity()) {
			throw new RKVDBException("Corrupted record at " + offset + " in file " + fileId);
		}

		Record record = Record.deserialize(recordBuf, header.getKeySize(), header.getValueSize());
		record.setHeader(header);
		int valueOffset = offset + Record.Header.HEADER_SIZE + header.getKeySize();
		record.setRecordMetaData(
				new InMemoryIndexMetaData(fileId, valueOffset, header.getValueSize(), header.getSequenceNumber()));
		return record;
	}

	InMemoryIndexMetaData writeRecord(Record record) throws IOException {
		writeToChannel(record.serialize());

		int recordSize = record.getRecordSize();
		int recordOffset = writeOffset;
		writeOffset += recordSize;

		IndexFileEntry indexFileEntry = new IndexFileEntry(record.getKey(), recordSize, recordOffset,
				record.getSequenceNumber(), Versions.CURRENT_INDEX_FILE_VERSION, -1);
		indexFile.write(indexFileEntry);

		int valueOffset = Utils.getValueOffset(recordOffset, record.getKey());
		return new InMemoryIndexMetaData(fileId, valueOffset, record.getValue().length, record.getSequenceNumber());
	}

	void rebuildIndexFile() throws IOException {
		indexFile.delete();

		indexFile = new IndexFile(fileId, dbDirectory, options);
		indexFile.create();

		HaloDBFileIterator iterator = new HaloDBFileIterator();
		int offset = 0;
		while (iterator.hasNext()) {
			Record record = iterator.next();
			IndexFileEntry indexFileEntry = new IndexFileEntry(record.getKey(), record.getRecordSize(), offset,
					record.getSequenceNumber(), Versions.CURRENT_INDEX_FILE_VERSION, -1);
			indexFile.write(indexFileEntry);
			offset += record.getRecordSize();
		}
	}

	RKVDBFile repairFile(DBDirectory dbDirectory) throws IOException {
		RKVDBFile repairFile = createRepairFile();

		HaloDBFileIterator iterator = new HaloDBFileIterator();
		int count = 0;
		while (iterator.hasNext()) {
			Record record = iterator.next();
			// if the header is corrupted iterator will return null.
			if (record != null && record.verifyChecksum()) {
				repairFile.writeRecord(record);
				count++;
			} else {
				break;
			}
		}

		repairFile.flushToDisk();
		repairFile.indexFile.flushToDisk();
		Files.move(repairFile.indexFile.getPath(), indexFile.getPath(), REPLACE_EXISTING, ATOMIC_MOVE);
		Files.move(repairFile.getPath(), getPath(), REPLACE_EXISTING, ATOMIC_MOVE);
		dbDirectory.syncMetaData();
		repairFile.close();
		close();
		return openForReading(dbDirectory, getPath().toFile(), fileType, options);
	}

	private RKVDBFile createRepairFile() throws IOException {
		File repairFile = dbDirectory.getPath().resolve(getName() + ".repair").toFile();
		while (!repairFile.createNewFile()) {
			repairFile.delete();
		}

		FileChannel channel = new RandomAccessFile(repairFile, "rw").getChannel();
		IndexFile indexFile = new IndexFile(fileId, dbDirectory, options);
		indexFile.createRepairFile();
		return new RKVDBFile(fileId, repairFile, dbDirectory, indexFile, fileType, channel, options);
	}

	private long writeToChannel(ByteBuffer[] buffers) throws IOException {
		long toWrite = 0;
		for (ByteBuffer buffer : buffers) {
			toWrite += buffer.remaining();
		}

		long written = 0;
		while (written < toWrite) {
			written += channel.write(buffers);
		}

		unFlushedData += written;

		if (options.isSyncWrite()
				|| (options.getFlushDataSizeBytes() != -1 && unFlushedData > options.getFlushDataSizeBytes())) {
			flushToDisk();
			unFlushedData = 0;
		}
		return written;
	}

	void flushToDisk() throws IOException {
		if (channel != null && channel.isOpen())
			channel.force(true);
	}

	long getWriteOffset() {
		return writeOffset;
	}

	void setWriteOffset(int writeOffset) {
		this.writeOffset = writeOffset;
	}

	long getSize() {
		return writeOffset;
	}

	IndexFile getIndexFile() {
		return indexFile;
	}

	FileChannel getChannel() {
		return channel;
	}

	FileType getFileType() {
		return fileType;
	}

	int getFileId() {
		return fileId;
	}

	static RKVDBFile openForReading(DBDirectory dbDirectory, File filename, FileType fileType, RKVDBOptions options)
			throws IOException {
		int fileId = RKVDBFile.getFileTimeStamp(filename);
		FileChannel channel = new RandomAccessFile(filename, "r").getChannel();
		IndexFile indexFile = new IndexFile(fileId, dbDirectory, options);
		indexFile.open();

		return new RKVDBFile(fileId, filename, dbDirectory, indexFile, fileType, channel, options);
	}

	static RKVDBFile create(DBDirectory dbDirectory, int fileId, RKVDBOptions options, FileType fileType)
			throws IOException {
		BiFunction<DBDirectory, Integer, File> toFile = (fileType == FileType.DATA_FILE) ? RKVDBFile::getDataFile
				: RKVDBFile::getCompactedDataFile;

		File file = toFile.apply(dbDirectory, fileId);
		while (!file.createNewFile()) {
			// file already exists try another one.
			fileId++;
			file = toFile.apply(dbDirectory, fileId);
		}

		FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
		// TODO: setting the length might improve performance.
		// file.setLength(max_);

		IndexFile indexFile = new IndexFile(fileId, dbDirectory, options);
		indexFile.create();

		return new RKVDBFile(fileId, file, dbDirectory, indexFile, fileType, channel, options);
	}

	HaloDBFileIterator newIterator() throws IOException {
		return new HaloDBFileIterator();
	}

	void close() throws IOException {
		if (channel != null) {
			channel.close();
		}
		if (indexFile != null) {
			indexFile.close();
		}
	}

	void delete() throws IOException {
		close();
		if (backingFile != null)
			backingFile.delete();

		if (indexFile != null)
			indexFile.delete();
	}

	String getName() {
		return backingFile.getName();
	}

	Path getPath() {
		return backingFile.toPath();
	}

	private static File getDataFile(DBDirectory dbDirectory, int fileId) {
		return dbDirectory.getPath().resolve(fileId + DATA_FILE_NAME).toFile();
	}

	private static File getCompactedDataFile(DBDirectory dbDirectory, int fileId) {
		return dbDirectory.getPath().resolve(fileId + COMPACTED_DATA_FILE_NAME).toFile();
	}

	static FileType findFileType(File file) {
		String name = file.getName();
		return name.endsWith(COMPACTED_DATA_FILE_NAME) ? FileType.COMPACTED_FILE : FileType.DATA_FILE;
	}

	static int getFileTimeStamp(File file) {
		Matcher matcher = Constants.DATA_FILE_PATTERN.matcher(file.getName());
		matcher.find();
		String s = matcher.group(1);
		return Integer.parseInt(s);
	}

	class HaloDBFileIterator implements Iterator<Record> {

		private final int endOffset;
		private int currentOffset = 0;

		HaloDBFileIterator() throws IOException {
			this.endOffset = Ints.checkedCast(channel.size());
		}

		@Override
		public boolean hasNext() {
			return currentOffset < endOffset;
		}

		@Override
		public Record next() {
			Record record;
			try {
				record = readRecord(currentOffset);
			} catch (IOException | RKVDBException e) {
				// we have encountered an error, probably because record is corrupted.
				// we skip rest of the file and return null.
				currentOffset = endOffset;
				return null;
			}
			currentOffset += record.getRecordSize();
			return record;
		}
	}

	enum FileType {
		DATA_FILE, COMPACTED_FILE;
	}
}
