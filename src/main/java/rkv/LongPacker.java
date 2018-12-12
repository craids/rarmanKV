
package rkv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class LongPacker {

	static public void packLong(DataOutput os, long value) throws IOException {

		if (value < 0) {
			throw new IllegalArgumentException("negative value: v=" + value);
		}

		while ((value & ~0x7FL) != 0) {
			os.write((((int) value & 0x7F) | 0x80));
			value >>>= 7;
		}
		os.write((byte) value);
	}

	static public long unpackLong(DataInput is) throws IOException {

		long result = 0;
		for (int offset = 0; offset < 64; offset += 7) {
			long b = is.readUnsignedByte();
			result |= (b & 0x7F) << offset;
			if ((b & 0x80) == 0) {
				return result;
			}
		}
		throw new Error("Malformed long.");
	}

	static public void packInt(DataOutput os, int value) throws IOException {

		if (value < 0) {
			throw new IllegalArgumentException("negative value: v=" + value);
		}

		while ((value & ~0x7F) != 0) {
			os.write(((value & 0x7F) | 0x80));
			value >>>= 7;
		}

		os.write((byte) value);
	}

	static public int unpackInt(DataInput is) throws IOException {
		for (int offset = 0, result = 0; offset < 32; offset += 7) {
			int b = is.readUnsignedByte();
			result |= (b & 0x7F) << offset;
			if ((b & 0x80) == 0) {
				return result;
			}
		}
		throw new Error("Malformed integer.");

	}

}
