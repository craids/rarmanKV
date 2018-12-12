
package rkv;

final class RecordHeader {
	// offsets
	private static final short O_CURRENTSIZE = 0; // int currentSize
	private static final short O_AVAILABLESIZE = Magic.SZ_BYTE; // int availableSize
	static final int MAX_RECORD_SIZE = 8355839;
	static final int SIZE = O_AVAILABLESIZE + Magic.SZ_SHORT;

	static final int MAX_SIZE_SPACE = 255 - 1;

	static int getCurrentSize(final PageIo page, final short pos) {
		int s = page.readByte(pos + O_CURRENTSIZE) & 0xFF;
		if (s == MAX_SIZE_SPACE + 1)
			return 0;
		return getAvailableSize(page, pos) - s;
	}

	static void setCurrentSize(final PageIo page, final short pos, int value) {
		if (value == 0) {
			page.writeByte(pos + O_CURRENTSIZE, (byte) (MAX_SIZE_SPACE + 1));
			return;
		}
		int availSize = getAvailableSize(page, pos);
		if (value < (availSize - MAX_SIZE_SPACE) || value > availSize)
			throw new IllegalArgumentException(
					"currentSize out of bounds, need to realocate " + value + " - " + availSize);
		page.writeByte(pos + O_CURRENTSIZE, (byte) (availSize - value));
	}

	static int getAvailableSize(final PageIo page, final short pos) {
		return deconvertAvailSize(page.readShort(pos + O_AVAILABLESIZE));
	}

	static void setAvailableSize(final PageIo page, final short pos, int value) {
		if (value != roundAvailableSize(value))
			throw new IllegalArgumentException("value is not rounded");
		int oldCurrSize = getCurrentSize(page, pos);

		page.writeShort(pos + O_AVAILABLESIZE, convertAvailSize(value));
		setCurrentSize(page, pos, oldCurrSize);
	}

	static short convertAvailSize(final int recordSize) {
		if (recordSize <= Short.MAX_VALUE)
			return (short) recordSize;
		else {
			int shift = recordSize - Short.MAX_VALUE;
			if (shift % MAX_SIZE_SPACE == 0)
				shift = shift / MAX_SIZE_SPACE;
			else
				shift = 1 + shift / MAX_SIZE_SPACE;
			shift = -shift;
			return (short) (shift);
		}

	}

	static int deconvertAvailSize(final short converted) {
		if (converted >= 0)
			return converted;
		else {
			int shifted = -converted;
			shifted = shifted * MAX_SIZE_SPACE;
			return Short.MAX_VALUE + shifted;
		}

	}

	static int roundAvailableSize(int value) {
		if (value > MAX_RECORD_SIZE)
			new InternalError("Maximal record size (" + MAX_RECORD_SIZE + ") exceeded: " + value);
		return deconvertAvailSize(convertAvailSize(value));
	}

}
