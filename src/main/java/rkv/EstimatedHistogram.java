
// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package rkv;

import com.google.common.base.Objects;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

public class EstimatedHistogram {

	private final long[] bucketOffsets;

	// buckets is one element longer than bucketOffsets -- the last element is
	// values greater than the last offset
	final AtomicLongArray buckets;

	public EstimatedHistogram() {
		this(90);
	}

	public EstimatedHistogram(int bucketCount) {
		bucketOffsets = newOffsets(bucketCount);
		buckets = new AtomicLongArray(bucketOffsets.length + 1);
	}

	public EstimatedHistogram(long[] offsets, long[] bucketData) {
		assert bucketData.length == offsets.length + 1;
		bucketOffsets = offsets;
		buckets = new AtomicLongArray(bucketData);
	}

	private static long[] newOffsets(int size) {
		long[] result = new long[size];
		long last = 1;
		result[0] = last;
		for (int i = 1; i < size; i++) {
			long next = Math.round(last * 1.2);
			if (next == last) {
				next++;
			}
			result[i] = next;
			last = next;
		}

		return result;
	}

	public long[] getBucketOffsets() {
		return bucketOffsets;
	}

	public void add(long n) {
		int index = Arrays.binarySearch(bucketOffsets, n);
		if (index < 0) {
			// inexact match, take the first bucket higher than n
			index = -index - 1;
		}
		// else exact match; we're good
		buckets.incrementAndGet(index);
	}

	long get(int bucket) {
		return buckets.get(bucket);
	}

	public long[] getBuckets(boolean reset) {
		final int len = buckets.length();
		long[] rv = new long[len];

		if (reset) {
			for (int i = 0; i < len; i++) {
				rv[i] = buckets.getAndSet(i, 0L);
			}
		} else {
			for (int i = 0; i < len; i++) {
				rv[i] = buckets.get(i);
			}
		}

		return rv;
	}

	public long min() {
		for (int i = 0; i < buckets.length(); i++) {
			if (buckets.get(i) > 0) {
				return i == 0 ? 0 : 1 + bucketOffsets[i - 1];
			}
		}
		return 0;
	}

	public long max() {
		int lastBucket = buckets.length() - 1;
		if (buckets.get(lastBucket) > 0) {
			return Long.MAX_VALUE;
		}

		for (int i = lastBucket - 1; i >= 0; i--) {
			if (buckets.get(i) > 0) {
				return bucketOffsets[i];
			}
		}
		return 0;
	}

	public long percentile(double percentile) {
		assert percentile >= 0 && percentile <= 1.0;
		int lastBucket = buckets.length() - 1;
		if (buckets.get(lastBucket) > 0) {
			throw new IllegalStateException("Unable to compute when histogram overflowed");
		}

		long pcount = (long) Math.floor(count() * percentile);
		if (pcount == 0) {
			return 0;
		}

		long elements = 0;
		for (int i = 0; i < lastBucket; i++) {
			elements += buckets.get(i);
			if (elements >= pcount) {
				return bucketOffsets[i];
			}
		}
		return 0;
	}

	public long mean() {
		int lastBucket = buckets.length() - 1;
		if (buckets.get(lastBucket) > 0) {
			throw new IllegalStateException("Unable to compute ceiling for max when histogram overflowed");
		}

		long elements = 0;
		long sum = 0;
		for (int i = 0; i < lastBucket; i++) {
			long bCount = buckets.get(i);
			elements += bCount;
			sum += bCount * bucketOffsets[i];
		}

		return (long) Math.ceil((double) sum / elements);
	}

	public long count() {
		long sum = 0L;
		for (int i = 0; i < buckets.length(); i++) {
			sum += buckets.get(i);
		}
		return sum;
	}

	public boolean isOverflowed() {
		return buckets.get(buckets.length() - 1) > 0;
	}

	public String toString() {
		// only print overflow if there is any
		int nameCount;
		if (buckets.get(buckets.length() - 1) == 0) {
			nameCount = buckets.length() - 1;
		} else {
			nameCount = buckets.length();
		}
		String[] names = new String[nameCount];

		int maxNameLength = 0;
		for (int i = 0; i < nameCount; i++) {
			names[i] = nameOfRange(bucketOffsets, i);
			maxNameLength = Math.max(maxNameLength, names[i].length());
		}

		StringBuilder sb = new StringBuilder();

		// emit log records
		String formatstr = "%" + maxNameLength + "s: %d\n";
		for (int i = 0; i < nameCount; i++) {
			long count = buckets.get(i);
			// sort-of-hack to not print empty ranges at the start that are only used to
			// demarcate the
			// first populated range. for code clarity we don't omit this record from the
			// maxNameLength
			// calculation, and accept the unnecessary whitespace prefixes that will
			// occasionally occur
			if (i == 0 && count == 0) {
				continue;
			}
			sb.append(String.format(formatstr, names[i], count));
		}

		return sb.toString();
	}

	private static String nameOfRange(long[] bucketOffsets, int index) {
		StringBuilder sb = new StringBuilder();
		appendRange(sb, bucketOffsets, index);
		return sb.toString();
	}

	private static void appendRange(StringBuilder sb, long[] bucketOffsets, int index) {
		sb.append("[");
		if (index == 0) {
			if (bucketOffsets[0] > 0)
			// by original definition, this histogram is for values greater than zero only;
			// if values of 0 or less are required, an entry of lb-1 must be inserted at the
			// start
			{
				sb.append("1");
			} else {
				sb.append("-Inf");
			}
		} else {
			sb.append(bucketOffsets[index - 1] + 1);
		}
		sb.append("..");
		if (index == bucketOffsets.length) {
			sb.append("Inf");
		} else {
			sb.append(bucketOffsets[index]);
		}
		sb.append("]");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (!(o instanceof EstimatedHistogram)) {
			return false;
		}

		EstimatedHistogram that = (EstimatedHistogram) o;
		return Arrays.equals(getBucketOffsets(), that.getBucketOffsets())
				&& Arrays.equals(getBuckets(false), that.getBuckets(false));
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(getBucketOffsets(), getBuckets(false));
	}
}
