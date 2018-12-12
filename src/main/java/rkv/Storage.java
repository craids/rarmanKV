package rkv;

import java.io.*;
import java.nio.ByteBuffer;

interface Storage {

	int PAGE_SIZE_SHIFT = 12;

	int PAGE_SIZE = 1 << PAGE_SIZE_SHIFT;

	long OFFSET_MASK = 0xFFFFFFFFFFFFFFFFL >>> (64 - Storage.PAGE_SIZE_SHIFT);

	void write(long pageNumber, ByteBuffer data) throws IOException;

	ByteBuffer read(long pageNumber) throws IOException;

	void forceClose() throws IOException;

	boolean isReadonly();

	DataInputStream readTransactionLog();

	void deleteTransactionLog();

	void sync() throws IOException;

	DataOutputStream openTransactionLog() throws IOException;

	void deleteAllFiles() throws IOException;
}
