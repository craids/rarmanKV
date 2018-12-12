
// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package rkv;

import java.io.Closeable;

interface OffHeapHashTable<V> extends Closeable {

	boolean put(byte[] key, V value);

	boolean addOrReplace(byte[] key, V old, V value);

	boolean putIfAbsent(byte[] key, V value);

	boolean remove(byte[] key);

	void clear();

	V get(byte[] key);

	boolean containsKey(byte[] key);

	// statistics / information

	void resetStatistics();

	long size();

	int[] hashTableSizes();

	SegmentStats[] perSegmentStats();

	EstimatedHistogram getBucketHistogram();

	int segments();

	float loadFactor();

	OffHeapHashTableStats stats();
}
