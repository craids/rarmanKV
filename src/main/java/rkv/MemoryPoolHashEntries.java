
package rkv;

class MemoryPoolHashEntries {

	static final int HEADER_SIZE = 1 + 4 + 1;

	static final int ENTRY_OFF_NEXT_CHUNK_INDEX = 0;
	static final int ENTRY_OFF_NEXT_CHUNK_OFFSET = 1;

	// offset of key length (1 bytes, byte)
	static final int ENTRY_OFF_KEY_LENGTH = 5;

	// offset of data in first block
	static final int ENTRY_OFF_DATA = 6;

}
