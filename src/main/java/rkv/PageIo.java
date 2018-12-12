
package rkv;

import javax.crypto.Cipher;

import static rkv.Magic.*;

import java.io.*;
import java.nio.ByteBuffer;

final class PageIo {

	private long pageId;

	private ByteBuffer data; // work area

	private boolean dirty = false;

	private int transactionCount = 0;

	public PageIo() {
		// empty
	}

	PageIo(long pageId, byte[] data) {
		this.pageId = pageId;
		this.data = ByteBuffer.wrap(data);
	}

	public PageIo(long pageId, ByteBuffer data) {
		this.pageId = pageId;
		this.data = data;
	}

	void ensureHeapBuffer() {
		if (data.isDirect()) {
			final byte[] bb = new byte[Storage.PAGE_SIZE];
			data.get(bb, 0, Storage.PAGE_SIZE);
			data = ByteBuffer.wrap(bb);
			if (data.isReadOnly())
				throw new InternalError();
		}

	}

	ByteBuffer getData() {
		return data;
	}

	long getPageId() {
		return pageId;
	}

	void setDirty() {
		dirty = true;

		if (data.isReadOnly()) {
			// make copy if needed, so we can write into buffer
			byte[] buf = new byte[Storage.PAGE_SIZE];
			data.get(buf, 0, Storage.PAGE_SIZE);
			data = ByteBuffer.wrap(buf);
		}
	}

	void setClean() {
		dirty = false;
	}

	boolean isDirty() {
		return dirty;
	}

	boolean isInTransaction() {
		return transactionCount != 0;
	}

	void incrementTransactionCount() {
		transactionCount++;
	}

	void decrementTransactionCount() {
		transactionCount--;
		if (transactionCount < 0)
			throw new Error("transaction count on page " + getPageId() + " below zero!");

	}

	public byte readByte(int pos) {
		return data.get(pos);
	}

	public void writeByte(int pos, byte value) {
		setDirty();
		data.put(pos, value);
	}

	public short readShort(int pos) {
		return data.getShort(pos);
	}

	public void writeShort(int pos, short value) {
		setDirty();
		data.putShort(pos, value);
	}

	public int readInt(int pos) {
		return data.getInt(pos);
	}

	public void writeInt(int pos, int value) {
		setDirty();
		data.putInt(pos, value);
	}

	public long readLong(int pos) {
		return data.getLong(pos);
	}

	public void writeLong(int pos, long value) {
		setDirty();
		data.putLong(pos, value);
	}

	public long readSixByteLong(int pos) {
		long ret = ((long) (data.get(pos + 0) & 0x7f) << 40) | ((long) (data.get(pos + 1) & 0xff) << 32)
				| ((long) (data.get(pos + 2) & 0xff) << 24) | ((long) (data.get(pos + 3) & 0xff) << 16)
				| ((long) (data.get(pos + 4) & 0xff) << 8) | ((long) (data.get(pos + 5) & 0xff) << 0);
		if ((data.get(pos + 0) & 0x80) != 0)
			return -ret;
		else
			return ret;

	}

	public void writeSixByteLong(int pos, long value) {
//        if(value<0) throw new IllegalArgumentException();
//    	if(value >> (6*8)!=0)
//    		throw new IllegalArgumentException("does not fit");
		int negativeBit = 0;
		if (value < 0) {
			value = -value;
			negativeBit = 0x80;
		}

		setDirty();
		data.put(pos + 0, (byte) ((0x7f & (value >> 40)) | negativeBit));
		data.put(pos + 1, (byte) (0xff & (value >> 32)));
		data.put(pos + 2, (byte) (0xff & (value >> 24)));
		data.put(pos + 3, (byte) (0xff & (value >> 16)));
		data.put(pos + 4, (byte) (0xff & (value >> 8)));
		data.put(pos + 5, (byte) (0xff & (value >> 0)));

	}

	// overrides java.lang.Object

	public String toString() {
		return "PageIo(" + pageId + "," + dirty + ")";
	}

	public void readExternal(DataInputStream in, Cipher cipherOut) throws IOException {
		pageId = in.readLong();
		byte[] data2 = new byte[Storage.PAGE_SIZE];
		in.readFully(data2);
		if (cipherOut == null || Utils.allZeros(data2))
			data = ByteBuffer.wrap(data2);
		else
			try {
				data = ByteBuffer.wrap(cipherOut.doFinal(data2));
			} catch (Exception e) {
				throw new IOError(e);
			}
	}

	public void writeExternal(DataOutput out, Cipher cipherIn) throws IOException {
		out.writeLong(pageId);
		out.write(Utils.encrypt(cipherIn, data.array()));
	}

	public byte[] getByteArray() {
		if (data.hasArray())
			return data.array();
		byte[] d = new byte[Storage.PAGE_SIZE];
		data.rewind();
		data.get(d, 0, Storage.PAGE_SIZE);
		return d;
	}

	public void writeByteArray(byte[] buf, int srcOffset, int offset, int length) {
		setDirty();
		data.rewind();
		data.position(offset);
		data.put(buf, srcOffset, length);
	}

	public void fileHeaderCheckHead(boolean isNew) {
		if (isNew)
			writeShort(FILE_HEADER_O_MAGIC, Magic.FILE_HEADER);
		else {
			short magic = readShort(FILE_HEADER_O_MAGIC);
			if (magic != FILE_HEADER)
				throw new Error("CRITICAL: file header magic not OK " + magic);
		}
	}

	long fileHeaderGetFirstOf(int list) {
		return readLong(fileHeaderOffsetOfFirst(list));
	}

	void fileHeaderSetFirstOf(int list, long value) {
		writeLong(fileHeaderOffsetOfFirst(list), value);
	}

	long fileHeaderGetLastOf(int list) {
		return readLong(fileHeaderOffsetOfLast(list));
	}

	void fileHeaderSetLastOf(int list, long value) {
		writeLong(fileHeaderOffsetOfLast(list), value);
	}

	private short fileHeaderOffsetOfFirst(int list) {
		return (short) (FILE_HEADER_O_LISTS + (2 * Magic.SZ_LONG * list));
	}

	private short fileHeaderOffsetOfLast(int list) {
		return (short) (fileHeaderOffsetOfFirst(list) + Magic.SZ_LONG);
	}

	long fileHeaderGetRoot(final int root) {
		final short offset = (short) (FILE_HEADER_O_ROOTS + (root * Magic.SZ_LONG));
		return readLong(offset);
	}

	void fileHeaderSetRoot(final int root, final long rowid) {
		final short offset = (short) (FILE_HEADER_O_ROOTS + (root * Magic.SZ_LONG));
		writeLong(offset, rowid);
	}

	boolean pageHeaderMagicOk() {
		int magic = pageHeaderGetMagic();
		return magic >= Magic.PAGE_MAGIC && magic <= (Magic.PAGE_MAGIC + Magic.FREEPHYSIDS_ROOT_PAGE);
	}

	protected void pageHeaderParanoiaMagicOk() {
		if (!pageHeaderMagicOk())
			throw new Error("CRITICAL: page header magic not OK " + pageHeaderGetMagic());
	}

	short pageHeaderGetMagic() {
		return readShort(PAGE_HEADER_O_MAGIC);
	}

	long pageHeaderGetNext() {
		pageHeaderParanoiaMagicOk();
		return readSixByteLong(PAGE_HEADER_O_NEXT);
	}

	void pageHeaderSetNext(long next) {
		pageHeaderParanoiaMagicOk();
		writeSixByteLong(PAGE_HEADER_O_NEXT, next);
	}

	long pageHeaderGetPrev() {
		pageHeaderParanoiaMagicOk();
		return readSixByteLong(PAGE_HEADER_O_PREV);
	}

	void pageHeaderSetPrev(long prev) {
		pageHeaderParanoiaMagicOk();
		writeSixByteLong(PAGE_HEADER_O_PREV, prev);
	}

	void pageHeaderSetType(short type) {
		writeShort(PAGE_HEADER_O_MAGIC, (short) (Magic.PAGE_MAGIC + type));
	}

	long pageHeaderGetLocation(final short pos) {
		return readSixByteLong(pos + PhysicalRowId_O_LOCATION);
	}

	void pageHeaderSetLocation(short pos, long value) {
		writeSixByteLong(pos + PhysicalRowId_O_LOCATION, value);
	}

	short dataPageGetFirst() {
		return readShort(DATA_PAGE_O_FIRST);
	}

	void dataPageSetFirst(short value) {
		pageHeaderParanoiaMagicOk();
		if (value > 0 && value < DATA_PAGE_O_DATA)
			throw new Error("DataPage.setFirst: offset " + value + " too small");
		writeShort(DATA_PAGE_O_FIRST, value);
	}

}
