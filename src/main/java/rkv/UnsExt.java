
// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package rkv;

import sun.misc.Unsafe;

abstract class UnsExt {

	final Unsafe unsafe;

	UnsExt(Unsafe unsafe) {
		this.unsafe = unsafe;
	}

	abstract long getAndPutLong(long address, long offset, long value);

	abstract int getAndAddInt(long address, long offset, int value);

	abstract long crc32(long address, long offset, long len);
}
