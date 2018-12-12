
package rkv;

import java.nio.ByteBuffer;

class InMemoryIndexMetaData {

	private final int fileId;
	private final int valueOffset;
	private final int valueSize;
	private final long sequenceNumber;

	static final int SERIALIZED_SIZE = 4 + 4 + 4 + 8;

	InMemoryIndexMetaData(int fileId, int valueOffset, int valueSize, long sequenceNumber) {
		this.fileId = fileId;
		this.valueOffset = valueOffset;
		this.valueSize = valueSize;
		this.sequenceNumber = sequenceNumber;
	}

	void serialize(ByteBuffer byteBuffer) {
		byteBuffer.putInt(getFileId());
		byteBuffer.putInt(getValueOffset());
		byteBuffer.putInt(getValueSize());
		byteBuffer.putLong(getSequenceNumber());
		byteBuffer.flip();
	}

	static InMemoryIndexMetaData deserialize(ByteBuffer byteBuffer) {
		int fileId = byteBuffer.getInt();
		int offset = byteBuffer.getInt();
		int size = byteBuffer.getInt();
		long sequenceNumber = byteBuffer.getLong();

		return new InMemoryIndexMetaData(fileId, offset, size, sequenceNumber);
	}

	int getFileId() {
		return fileId;
	}

	int getValueOffset() {
		return valueOffset;
	}

	int getValueSize() {
		return valueSize;
	}

	long getSequenceNumber() {
		return sequenceNumber;
	}
}
