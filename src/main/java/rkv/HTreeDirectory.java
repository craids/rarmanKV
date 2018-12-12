
package rkv;

import java.io.*;
import java.util.*;

final class HTreeDirectory<K, V> {

	static final int MAX_CHILDREN = 256;

	static final int BIT_SIZE = 8; // log2(256) = 8

	static final int MAX_DEPTH = 3; // 4 levels

	private long[][] _children;

	private byte _depth;

	private long _recid;

	long size;

	protected final HTree<K, V> tree;

	public HTreeDirectory(HTree<K, V> tree) {
		this.tree = tree;
	}

	HTreeDirectory(HTree<K, V> tree, byte depth) {
		this.tree = tree;
		_depth = depth;
		_children = new long[32][];
	}

	void setPersistenceContext(long recid) {
		this._recid = recid;
	}

	long getRecid() {
		return _recid;
	}

	boolean isEmpty() {
		for (int i = 0; i < _children.length; i++) {
			long[] sub = _children[i];
			if (sub != null) {
				for (int j = 0; j < 8; j++) {
					if (sub[j] != 0) {
						return false;
					}
				}
			}
		}
		return true;
	}

	V get(K key) throws IOException {
		int hash = hashCode(key);
		long child_recid = getRecid(hash);
		if (child_recid == 0) {
			// not bucket/node --> not found
			return null;
		} else {
			Object node = tree.db.fetch(child_recid, tree.SERIALIZER);
			// System.out.println("HashDirectory.get() child is : "+node);

			if (node instanceof HTreeDirectory) {
				// recurse into next directory level
				HTreeDirectory<K, V> dir = (HTreeDirectory<K, V>) node;
				dir.setPersistenceContext(child_recid);
				return dir.get(key);
			} else {
				// node is a bucket
				HTreeBucket<K, V> bucket = (HTreeBucket) node;
				return bucket.getValue(key);
			}
		}
	}

	private long getRecid(int hash) {
		long[] sub = _children[hash >>> 3];
		return sub == null ? 0 : sub[hash % 8];
	}

	private void putRecid(int hash, long recid) {
		long[] sub = _children[hash >>> 3];
		if (sub == null) {
			sub = new long[8];
			_children[hash >>> 3] = sub;
		}
		sub[hash % 8] = recid;
	}

	Object put(final Object key, final Object value) throws IOException {
		if (value == null) {
			return remove(key);
		}
		int hash = hashCode(key);
		long child_recid = getRecid(hash);
		if (child_recid == 0) {
			// no bucket/node here yet, let's create a bucket
			HTreeBucket bucket = new HTreeBucket(tree, (byte) (_depth + 1));

			// insert (key,value) pair in bucket
			Object existing = bucket.addElement(key, value);

			long b_recid = tree.db.insert(bucket, tree.SERIALIZER, false);
			putRecid(hash, b_recid);

			tree.db.update(_recid, this, tree.SERIALIZER);

			// System.out.println("Added: "+bucket);
			return existing;
		} else {
			Object node = tree.db.fetch(child_recid, tree.SERIALIZER);

			if (node instanceof HTreeDirectory) {
				// recursive insert in next directory level
				HTreeDirectory dir = (HTreeDirectory) node;
				dir.setPersistenceContext(child_recid);
				return dir.put(key, value);
			} else {
				// node is a bucket
				HTreeBucket bucket = (HTreeBucket) node;
				if (bucket.hasRoom()) {
					Object existing = bucket.addElement(key, value);
					tree.db.update(child_recid, bucket, tree.SERIALIZER);
					// System.out.println("Added: "+bucket);
					return existing;
				} else {
					// overflow, so create a new directory
					if (_depth == MAX_DEPTH) {
						throw new RuntimeException("Cannot create deeper directory. " + "Depth=" + _depth);
					}
					HTreeDirectory dir = new HTreeDirectory(tree, (byte) (_depth + 1));
					long dir_recid = tree.db.insert(dir, tree.SERIALIZER, false);
					dir.setPersistenceContext(dir_recid);

					putRecid(hash, dir_recid);
					tree.db.update(_recid, this, tree.SERIALIZER);

					// discard overflown bucket
					tree.db.delete(child_recid);

					// migrate existing bucket elements
					ArrayList keys = bucket.getKeys();
					ArrayList values = bucket.getValues();
					int entries = keys.size();
					for (int i = 0; i < entries; i++) {
						dir.put(keys.get(i), values.get(i));
					}

					// (finally!) insert new element
					return dir.put(key, value);
				}
			}
		}
	}

	Object remove(Object key) throws IOException {
		int hash = hashCode(key);
		long child_recid = getRecid(hash);
		if (child_recid == 0) {
			// not bucket/node --> not found
			return null;
		} else {
			Object node = tree.db.fetch(child_recid, tree.SERIALIZER);
			// System.out.println("HashDirectory.remove() child is : "+node);

			if (node instanceof HTreeDirectory) {
				// recurse into next directory level
				HTreeDirectory dir = (HTreeDirectory) node;
				dir.setPersistenceContext(child_recid);
				Object existing = dir.remove(key);
				if (existing != null) {
					if (dir.isEmpty()) {
						// delete empty directory
						tree.db.delete(child_recid);
						putRecid(hash, 0);
						tree.db.update(_recid, this, tree.SERIALIZER);
					}
				}
				return existing;
			} else {
				// node is a bucket
				HTreeBucket bucket = (HTreeBucket) node;
				Object existing = bucket.removeElement(key);
				if (existing != null) {
					if (bucket.getElementCount() >= 1) {
						tree.db.update(child_recid, bucket, tree.SERIALIZER);
					} else {
						// delete bucket, it's empty
						tree.db.delete(child_recid);
						putRecid(hash, 0);
						tree.db.update(_recid, this, tree.SERIALIZER);
					}
				}
				return existing;
			}
		}
	}

	private int hashCode(Object key) {
		int hashMask = hashMask();
		int hash = key.hashCode();
		hash = hash & hashMask;
		hash = hash >>> ((MAX_DEPTH - _depth) * BIT_SIZE);
		hash = hash % MAX_CHILDREN;

		return hash;
	}

	int hashMask() {
		int bits = MAX_CHILDREN - 1;
		int hashMask = bits << ((MAX_DEPTH - _depth) * BIT_SIZE);

		return hashMask;
	}

	Iterator<K> keys() throws IOException {
		return new HDIterator(true);
	}

	Iterator<V> values() throws IOException {
		return new HDIterator(false);
	}

	public void writeExternal(DataOutput out) throws IOException {
		out.writeByte(_depth);
		if (_depth == 0) {
			LongPacker.packLong(out, size);
		}

		int zeroStart = 0;
		for (int i = 0; i < MAX_CHILDREN; i++) {
			if (getRecid(i) != 0) {
				zeroStart = i;
				break;
			}
		}

		out.write(zeroStart);
		if (zeroStart == MAX_CHILDREN)
			return;

		int zeroEnd = 0;
		for (int i = MAX_CHILDREN - 1; i >= 0; i--) {
			if (getRecid(i) != 0) {
				zeroEnd = i;
				break;
			}
		}
		out.write(zeroEnd);

		for (int i = zeroStart; i <= zeroEnd; i++) {
			LongPacker.packLong(out, getRecid(i));
		}
	}

	public void readExternal(DataInputOutput in) throws IOException, ClassNotFoundException {
		_depth = in.readByte();
		if (_depth == 0)
			size = LongPacker.unpackLong(in);
		else
			size = -1;

		_children = new long[32][];
		int zeroStart = in.readUnsignedByte();
		int zeroEnd = in.readUnsignedByte();

		for (int i = zeroStart; i <= zeroEnd; i++) {
			long recid = LongPacker.unpackLong(in);
			if (recid != 0)
				putRecid(i, recid);
		}

	}

	public void defrag(DBStore r1, DBStore r2) throws IOException, ClassNotFoundException {
		for (long[] sub : _children) {
			if (sub == null)
				continue;
			for (long child : sub) {
				if (child == 0)
					continue;
				byte[] data = r1.fetchRaw(child);
				r2.forceInsert(child, data);
				Object t = tree.SERIALIZER.deserialize(new DataInputOutput(data));
				if (t instanceof HTreeDirectory) {
					((HTreeDirectory) t).defrag(r1, r2);
				}
			}
		}
	}

	void deleteAllChildren() throws IOException {
		for (long[] ll : _children) {
			if (ll != null) {
				for (long l : ll) {
					if (l != 0) {
						tree.db.delete(l);
					}
				}
			}
		}

	}

	////////////////////////////////////////////////////////////////////////
	// INNER CLASS
	////////////////////////////////////////////////////////////////////////

	class HDIterator<A> implements Iterator<A> {

		private boolean _iterateKeys;

		private ArrayList _dirStack;
		private ArrayList _childStack;

		private HTreeDirectory _dir;

		private int _child;

		private Iterator<A> _iter;

		private A next;

		private A last;

		private int expectedModCount;

		HDIterator(boolean iterateKeys) throws IOException {
			_dirStack = new ArrayList();
			_childStack = new ArrayList();
			_dir = HTreeDirectory.this;
			_child = -1;
			_iterateKeys = iterateKeys;
			expectedModCount = tree.modCount;

			prepareNext();
			next = next2();

		}

		public A next2() {
			A next = null;
			if (_iter != null && _iter.hasNext()) {
				next = _iter.next();
			} else {
				try {
					prepareNext();
				} catch (IOException except) {
					throw new IOError(except);
				}
				if (_iter != null && _iter.hasNext()) {
					return next2();
				}
			}
			return next;
		}

		private void prepareNext() throws IOException {
			long child_recid = 0;

			// get next bucket/directory to enumerate
			do {
				_child++;
				if (_child >= MAX_CHILDREN) {

					if (_dirStack.isEmpty()) {
						// no more directory in the stack, we're finished
						return;
					}

					// try next node
					_dir = (HTreeDirectory) _dirStack.remove(_dirStack.size() - 1);
					_child = ((Integer) _childStack.remove(_childStack.size() - 1)).intValue();
					continue;
				}
				child_recid = _dir.getRecid(_child);
			} while (child_recid == 0);

			if (child_recid == 0) {
				throw new Error("child_recid cannot be 0");
			}

			Object node = tree.db.fetch(child_recid, tree.SERIALIZER);
			// System.out.println("HDEnumeration.get() child is : "+node);

			if (node instanceof HTreeDirectory) {
				// save current position
				_dirStack.add(_dir);
				_childStack.add(new Integer(_child));

				_dir = (HTreeDirectory) node;
				_child = -1;

				// recurse into
				_dir.setPersistenceContext(child_recid);
				prepareNext();
			} else {
				// node is a bucket
				HTreeBucket bucket = (HTreeBucket) node;
				if (_iterateKeys) {
					ArrayList keys2 = bucket.getKeys();
					_iter = keys2.iterator();
				} else {
					_iter = bucket.getValues().iterator();
				}
			}
		}

		public boolean hasNext() {
			return next != null;
		}

		public A next() {
			if (next == null)
				throw new NoSuchElementException();
			if (expectedModCount != tree.modCount)
				throw new ConcurrentModificationException();
			last = next;
			next = next2();
			return last;
		}

		public void remove() {
			if (last == null)
				throw new IllegalStateException();

			if (expectedModCount != tree.modCount)
				throw new ConcurrentModificationException();

			// TODO current delete behaviour may change node layout. INVESTIGATE if this can
			// happen!
			tree.remove(last);
			last = null;
			expectedModCount++;
		}
	}

}
