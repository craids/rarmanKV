
// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package rkv;

import com.sun.jna.*;

final class JNANativeAllocator implements NativeMemoryAllocator {

	public long allocate(long size) {
		try {
			return Native.malloc(size);
		} catch (OutOfMemoryError oom) {
			return 0L;
		}
	}

	public void free(long peer) {
		Native.free(peer);
	}

	public long getTotalAllocated() {
		return -1L;
	}
}
