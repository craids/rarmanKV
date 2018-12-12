package rkv;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class BTree<K, V> {

	private static final boolean DEBUG = false;

	public static final int DEFAULT_SIZE = 32; // TODO test optimal size, it has serious impact on sequencial write and
												// read

	protected transient DBAbstract _db;

	private transient long _recid;

	protected Comparator<K> _comparator;

	protected Serializer<K> keySerializer;

	protected Serializer<V> valueSerializer;

	boolean loadValues = true;

	boolean hasValues = true;

	transient int modCount = 0;

	protected BTreeNode.InsertResult<K, V> insertResultReuse; // TODO investigate performance impact of removing this

	public Serializer<K> getKeySerializer() {
		return keySerializer;
	}

	public Serializer<V> getValueSerializer() {
		return valueSerializer;
	}

	private int _height;

	private transient long _root;

	protected volatile long _entries;

	private transient BTreeNode<K, V> _nodeSerializer = new BTreeNode();
	{
		_nodeSerializer._btree = this;
	}

	protected RecordListener[] recordListeners = new RecordListener[0];

	final protected ReadWriteLock lock = new ReentrantReadWriteLock();

	public BTree() {
		// empty
	}

	@SuppressWarnings("unchecked")
	public static <K extends Comparable, V> BTree<K, V> createInstance(DBAbstract db) throws IOException {
		return createInstance(db, null, null, null, true);
	}

	public static <K, V> BTree<K, V> createInstance(DBAbstract db, Comparator<K> comparator,
			Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean hasValues) throws IOException {
		BTree<K, V> btree;

		if (db == null) {
			throw new IllegalArgumentException("Argument 'db' is null");
		}

		btree = new BTree<K, V>();
		btree._db = db;
		btree._comparator = comparator;
		btree.keySerializer = keySerializer;
		btree.valueSerializer = valueSerializer;
		btree.hasValues = hasValues;
		btree._recid = db.insert(btree, btree.getRecordManager().defaultSerializer(), false);

		return btree;
	}

	@SuppressWarnings("unchecked")
	public static <K, V> BTree<K, V> load(DBAbstract db, long recid) throws IOException {
		BTree<K, V> btree = (BTree<K, V>) db.fetch(recid);
		btree._recid = recid;
		btree._db = db;
		btree._nodeSerializer = new BTreeNode<K, V>();
		btree._nodeSerializer._btree = btree;
		return btree;
	}

	public ReadWriteLock getLock() {
		return lock;
	}

	public V insert(final K key, final V value, final boolean replace) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException("Argument 'key' is null");
		}
		if (value == null) {
			throw new IllegalArgumentException("Argument 'value' is null");
		}
		try {
			lock.writeLock().lock();
			BTreeNode<K, V> rootNode = getRoot();

			if (rootNode == null) {
				// BTree is currently empty, create a new root BTreeNode
				if (DEBUG) {
					System.out.println("BTree.insert() new root BTreeNode");
				}
				rootNode = new BTreeNode<K, V>(this, key, value);
				_root = rootNode._recid;
				_height = 1;
				_entries = 1;
				_db.update(_recid, this);
				modCount++;
				// notifi listeners
				for (RecordListener<K, V> l : recordListeners) {
					l.recordInserted(key, value);
				}
				return null;
			} else {
				BTreeNode.InsertResult<K, V> insert = rootNode.insert(_height, key, value, replace);
				boolean dirty = false;
				if (insert._overflow != null) {
					// current root node overflowed, we replace with a new root node
					if (DEBUG) {
						System.out.println("BTreeNode.insert() replace root BTreeNode due to overflow");
					}
					rootNode = new BTreeNode<K, V>(this, rootNode, insert._overflow);
					_root = rootNode._recid;
					_height += 1;
					dirty = true;
				}
				if (insert._existing == null) {
					_entries++;
					modCount++;
					dirty = true;
				}
				if (dirty) {
					_db.update(_recid, this);
				}
				// notify listeners
				for (RecordListener<K, V> l : recordListeners) {
					if (insert._existing == null)
						l.recordInserted(key, value);
					else
						l.recordUpdated(key, insert._existing, value);
				}

				// insert might have returned an existing value
				V ret = insert._existing;
				// zero out tuple and put it for reuse
				insert._existing = null;
				insert._overflow = null;
				this.insertResultReuse = insert;
				return ret;
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	public V remove(K key) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException("Argument 'key' is null");
		}
		try {
			lock.writeLock().lock();
			BTreeNode<K, V> rootNode = getRoot();
			if (rootNode == null) {
				return null;
			}
			boolean dirty = false;
			BTreeNode.RemoveResult<K, V> remove = rootNode.remove(_height, key);
			if (remove._underflow && rootNode.isEmpty()) {
				_height -= 1;
				dirty = true;

				_db.delete(_root);
				if (_height == 0) {
					_root = 0;
				} else {
					_root = rootNode.loadLastChildNode()._recid;
				}
			}
			if (remove._value != null) {
				_entries--;
				modCount++;
				dirty = true;
			}
			if (dirty) {
				_db.update(_recid, this);
			}
			if (remove._value != null)
				for (RecordListener<K, V> l : recordListeners)
					l.recordRemoved(key, remove._value);
			return remove._value;
		} finally {
			lock.writeLock().unlock();
		}
	}

	public V get(K key) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException("Argument 'key' is null");
		}
		try {
			lock.readLock().lock();
			BTreeNode<K, V> rootNode = getRoot();
			if (rootNode == null) {
				return null;
			}

			return rootNode.findValue(_height, key);
		} finally {
			lock.readLock().unlock();
		}
	}

	public BTreeTuple<K, V> findGreaterOrEqual(K key) throws IOException {
		BTreeTuple<K, V> tuple;
		BTreeTupleBrowser<K, V> browser;

		if (key == null) {
			// there can't be a key greater than or equal to "null"
			// because null is considered an infinite key.
			return null;
		}

		tuple = new BTreeTuple<K, V>(null, null);
		browser = browse(key, true);
		if (browser.getNext(tuple)) {
			return tuple;
		} else {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public BTreeTupleBrowser<K, V> browse() throws IOException {
		try {
			lock.readLock().lock();
			BTreeNode<K, V> rootNode = getRoot();
			if (rootNode == null) {
				return EMPTY_BROWSER;
			}
			return rootNode.findFirst();
		} finally {
			lock.readLock().unlock();
		}
	}

	@SuppressWarnings("unchecked")
	public BTreeTupleBrowser<K, V> browse(final K key, final boolean inclusive) throws IOException {
		try {
			lock.readLock().lock();
			BTreeNode<K, V> rootNode = getRoot();
			if (rootNode == null) {
				return EMPTY_BROWSER;
			}
			BTreeTupleBrowser<K, V> browser = rootNode.find(_height, key, inclusive);
			return browser;
		} finally {
			lock.readLock().unlock();
		}
	}

	public long getRecid() {
		return _recid;
	}

	BTreeNode<K, V> getRoot() throws IOException {
		if (_root == 0) {
			return null;
		}
		BTreeNode<K, V> root = _db.fetch(_root, _nodeSerializer);
		if (root != null) {
			root._recid = _root;
			root._btree = this;
		}
		return root;
	}

	static BTree readExternal(DataInput in, Serialization ser) throws IOException, ClassNotFoundException {
		BTree tree = new BTree();
		tree._db = ser.db;
		tree._height = in.readInt();
		tree._recid = in.readLong();
		tree._root = in.readLong();
		tree._entries = in.readLong();
		tree.hasValues = in.readBoolean();
		tree._comparator = (Comparator) ser.deserialize(in);
		tree.keySerializer = (Serializer) ser.deserialize(in);
		tree.valueSerializer = (Serializer) ser.deserialize(in);
		return tree;
	}

	public void writeExternal(DataOutput out) throws IOException {
		out.writeInt(_height);
		out.writeLong(_recid);
		out.writeLong(_root);
		out.writeLong(_entries);
		out.writeBoolean(hasValues);
		_db.defaultSerializer().serialize(out, _comparator);
		_db.defaultSerializer().serialize(out, keySerializer);
		_db.defaultSerializer().serialize(out, valueSerializer);
	}

	public static void defrag(long recid, DBStore r1, DBStore r2) throws IOException {
		try {
			byte[] data = r1.fetchRaw(recid);
			r2.forceInsert(recid, data);
			DataInput in = new DataInputOutput(data);
			BTree t = (BTree) r1.defaultSerializer().deserialize(in);
			t.loadValues = false;
			t._db = r1;
			t._nodeSerializer = new BTreeNode(t, false);

			BTreeNode p = t.getRoot();
			if (p != null) {
				r2.forceInsert(t._root, r1.fetchRaw(t._root));
				p.defrag(r1, r2);
			}

		} catch (ClassNotFoundException e) {
			throw new IOError(e);
		}
	}

	private static final BTreeTupleBrowser EMPTY_BROWSER = new BTreeTupleBrowser() {

		public boolean getNext(BTreeTuple tuple) {
			return false;
		}

		public boolean getPrevious(BTreeTuple tuple) {
			return false;
		}

		public void remove(Object key) {
			throw new IndexOutOfBoundsException();
		}
	};

	public void addRecordListener(RecordListener<K, V> listener) {
		recordListeners = Arrays.copyOf(recordListeners, recordListeners.length + 1);
		recordListeners[recordListeners.length - 1] = listener;
	}

	public void removeRecordListener(RecordListener<K, V> listener) {
		List l = Arrays.asList(recordListeners);
		l.remove(listener);
		recordListeners = (RecordListener[]) l.toArray(new RecordListener[1]);
	}

	public DBAbstract getRecordManager() {
		return _db;
	}

	public Comparator<K> getComparator() {
		return _comparator;
	}

	public void clear() throws IOException {
		try {
			lock.writeLock().lock();
			BTreeNode<K, V> rootNode = getRoot();
			if (rootNode != null)
				rootNode.delete();
			_entries = 0;
			modCount++;
		} finally {
			lock.writeLock().unlock();
		}
	}

	void dumpChildNodeRecIDs(List<Long> out) throws IOException {
		BTreeNode<K, V> root = getRoot();
		if (root != null) {
			out.add(root._recid);
			root.dumpChildNodeRecIDs(out, _height);
		}
	}

	public boolean hasValues() {
		return hasValues;
	}

	static interface BTreeTupleBrowser<K, V> {

		boolean getNext(BTree.BTreeTuple<K, V> tuple) throws IOException;

		boolean getPrevious(BTree.BTreeTuple<K, V> tuple) throws IOException;

		void remove(K key) throws IOException;

	}

	static final class BTreeTuple<K, V> {

		K key;

		V value;

		BTreeTuple() {
			// empty
		}

		BTreeTuple(K key, V value) {
			this.key = key;
			this.value = value;
		}

	}

}
