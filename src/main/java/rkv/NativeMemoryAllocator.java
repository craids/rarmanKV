
//This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package rkv;

interface NativeMemoryAllocator {

	long allocate(long size);

	void free(long peer);

	long getTotalAllocated();
}
