
package rkv;

class MemoryPoolAddress {

	final byte chunkIndex;
	final int chunkOffset;

	MemoryPoolAddress(byte chunkIndex, int chunkOffset) {
		this.chunkIndex = chunkIndex;
		this.chunkOffset = chunkOffset;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof MemoryPoolAddress)) {
			return false;
		}
		MemoryPoolAddress m = (MemoryPoolAddress) o;
		return m.chunkIndex == chunkIndex && m.chunkOffset == chunkOffset;
	}

	@Override
	public int hashCode() {
		return 31 * ((31 * chunkIndex) + chunkOffset);
	}
}
