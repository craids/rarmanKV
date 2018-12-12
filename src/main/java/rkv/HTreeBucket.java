
package rkv;

import java.io.*;
import java.util.ArrayList;

final class HTreeBucket<K, V> {

	public static final int OVERFLOW_SIZE = 16;

	private byte _depth;

	private Object[] _keysAndValues;

	private byte size = 0;

	private final HTree<K, V> tree;

	public HTreeBucket(HTree<K, V> tree) {
		this.tree = tree;
	}

	public HTreeBucket(HTree<K, V> tree, byte level) {
		this.tree = tree;
		if (level > HTreeDirectory.MAX_DEPTH + 1) {
			throw new IllegalArgumentException("Cannot create bucket with depth > MAX_DEPTH+1. " + "Depth=" + level);
		}
		_depth = level;
		_keysAndValues = new Object[OVERFLOW_SIZE * 2];
	}

	public int getElementCount() {
		return size;
	}

	public boolean isLeaf() {
		return (_depth > HTreeDirectory.MAX_DEPTH);
	}

	public boolean hasRoom() {
		if (isLeaf()) {
			return true; // leaf buckets are never full
		} else {
			// non-leaf bucket
			return (size < OVERFLOW_SIZE);
		}
	}

	public V addElement(K key, V value) {
		// find entry
		byte existing = -1;
		for (byte i = 0; i < size; i++) {
			if (key.equals(_keysAndValues[i])) {
				existing = i;
				break;
			}
		}

		if (existing != -1) {
			// replace existing element
			Object before = _keysAndValues[existing + OVERFLOW_SIZE];
			if (before instanceof BTreeLazyRecord) {
				BTreeLazyRecord<V> rec = (BTreeLazyRecord<V>) before;
				before = rec.get();
				rec.delete();
			}
			_keysAndValues[existing + OVERFLOW_SIZE] = value;
			return (V) before;
		} else {
			// add new (key, value) pair
			_keysAndValues[size] = key;
			_keysAndValues[size + OVERFLOW_SIZE] = value;
			size++;
			return null;
		}
	}

	public V removeElement(K key) {
		// find entry
		byte existing = -1;
		for (byte i = 0; i < size; i++) {
			if (key.equals(_keysAndValues[i])) {
				existing = i;
				break;
			}
		}

		if (existing != -1) {
			Object o = _keysAndValues[existing + OVERFLOW_SIZE];
			if (o instanceof BTreeLazyRecord) {
				BTreeLazyRecord<V> rec = (BTreeLazyRecord<V>) o;
				o = rec.get();
				rec.delete();
			}

			// move last element to existing
			size--;
			_keysAndValues[existing] = _keysAndValues[size];
			_keysAndValues[existing + OVERFLOW_SIZE] = _keysAndValues[size + OVERFLOW_SIZE];

			// and unset last element
			_keysAndValues[size] = null;
			_keysAndValues[size + OVERFLOW_SIZE] = null;

			return (V) o;
		} else {
			// not found
			return null;
		}
	}

	public V getValue(K key) {
		// find entry
		byte existing = -1;
		for (byte i = 0; i < size; i++) {
			if (key.equals(_keysAndValues[i])) {
				existing = i;
				break;
			}
		}

		if (existing != -1) {
			Object o = _keysAndValues[existing + OVERFLOW_SIZE];
			if (o instanceof BTreeLazyRecord)
				return ((BTreeLazyRecord<V>) o).get();
			else
				return (V) o;
		} else {
			// key not found
			return null;
		}
	}

	ArrayList<K> getKeys() {
		ArrayList<K> ret = new ArrayList<K>();
		for (byte i = 0; i < size; i++) {
			ret.add((K) _keysAndValues[i]);
		}
		return ret;
	}

	ArrayList<V> getValues() {
		ArrayList<V> ret = new ArrayList<V>();
		for (byte i = 0; i < size; i++) {
			ret.add((V) _keysAndValues[i + OVERFLOW_SIZE]);
		}
		return ret;

	}

	public void writeExternal(DataOutput out) throws IOException {
		out.write(_depth);
		out.write(size);

		DataInputOutput out3 = tree.writeBufferCache.getAndSet(null);
		if (out3 == null)
			out3 = new DataInputOutput();
		else
			out3.reset();

		Serializer keySerializer = tree.keySerializer != null ? tree.keySerializer
				: tree.getRecordManager().defaultSerializer();
		for (byte i = 0; i < size; i++) {
			out3.reset();
			keySerializer.serialize(out3, _keysAndValues[i]);
			LongPacker.packInt(out, out3.getPos());
			out.write(out3.getBuf(), 0, out3.getPos());

		}

		// write values
		if (tree.hasValues()) {
			Serializer valSerializer = tree.valueSerializer != null ? tree.valueSerializer
					: tree.getRecordManager().defaultSerializer();

			for (byte i = 0; i < size; i++) {
				Object value = _keysAndValues[i + OVERFLOW_SIZE];
				if (value == null) {
					out.write(BTreeLazyRecord.NULL);
				} else if (value instanceof BTreeLazyRecord) {
					out.write(BTreeLazyRecord.LAZY_RECORD);
					LongPacker.packLong(out, ((BTreeLazyRecord) value).recid);
				} else {
					// transform to byte array
					out3.reset();
					valSerializer.serialize(out3, value);

					if (out3.getPos() > BTreeLazyRecord.MAX_INTREE_RECORD_SIZE) {
						// store as separate record
						long recid = tree.getRecordManager().insert(out3.toByteArray(), BTreeLazyRecord.FAKE_SERIALIZER,
								true);
						out.write(BTreeLazyRecord.LAZY_RECORD);
						LongPacker.packLong(out, recid);
					} else {
						out.write(out3.getPos());
						out.write(out3.getBuf(), 0, out3.getPos());
					}
				}
			}
		}
		tree.writeBufferCache.set(out3);

	}

	public void readExternal(DataInputOutput in) throws IOException, ClassNotFoundException {
		_depth = in.readByte();
		size = in.readByte();

		// read keys
		Serializer keySerializer = tree.keySerializer != null ? tree.keySerializer
				: tree.getRecordManager().defaultSerializer();
		_keysAndValues = (K[]) new Object[OVERFLOW_SIZE * 2];
		for (byte i = 0; i < size; i++) {
			int expectedSize = LongPacker.unpackInt(in);
			K key = (K) BTreeLazyRecord.fastDeser(in, keySerializer, expectedSize);
			_keysAndValues[i] = key;
		}

		// read values
		if (tree.hasValues()) {
			Serializer<V> valSerializer = tree.valueSerializer != null ? tree.valueSerializer
					: (Serializer<V>) tree.getRecordManager().defaultSerializer();
			for (byte i = 0; i < size; i++) {
				int header = in.readUnsignedByte();
				if (header == BTreeLazyRecord.NULL) {
					_keysAndValues[i + OVERFLOW_SIZE] = null;
				} else if (header == BTreeLazyRecord.LAZY_RECORD) {
					long recid = LongPacker.unpackLong(in);
					_keysAndValues[i + OVERFLOW_SIZE] = (new BTreeLazyRecord(tree.getRecordManager(), recid,
							valSerializer));
				} else {
					_keysAndValues[i + OVERFLOW_SIZE] = BTreeLazyRecord.fastDeser(in, valSerializer, header);
				}
			}
		} else {
			for (byte i = 0; i < size; i++) {
				if (_keysAndValues[i] != null)
					_keysAndValues[i + OVERFLOW_SIZE] = Utils.EMPTY_STRING;
			}
		}
	}
}
