package core;

import java.util.TreeMap;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;

public class SimpleKV implements KeyValue {

	private final String storeFile = "rkv.dat";
	private TreeMap<String, String> map;

	@SuppressWarnings("unchecked")
	public SimpleKV() {
		File mapstore = new File("rkv.dat");
		if (mapstore.exists()) {
			try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(mapstore))) {
				map = (TreeMap<String, String>) ois.readObject();
			} catch (Exception ex) {
				this.map = new TreeMap<>();
				return;
			}
		} else
			this.map = new TreeMap<>();
	}

	@Override
	public SimpleKV initAndMakeStore(String path) {
		return new SimpleKV();
	}

	@Override
	public void write(char[] key, char[] value) {
		map.put(new String(key), new String(value));
	}

	@Override
	public char[] read(char[] key) {
		return map.get(new String(key)).toCharArray();
	}

	@Override
	public Iterator<KVPair> readRange(char[] startKey, char[] endKey) {
		String start = new String(startKey);
		String end = new String(endKey);
		return new KVPairIterator(start, end);
	}

	private class KVPairIterator implements Iterator<KVPair> {
		private Iterator<String> ksIterator;

		public KVPairIterator(String start, String end) {
			ksIterator = map.navigableKeySet().subSet(start, true, end, true).iterator();
		}

		@Override
		public boolean hasNext() {
			return ksIterator.hasNext();
		}

		@Override
		public KVPair next() {
			String nextKey = ksIterator.next();
			return new KVPair(nextKey.toCharArray(), map.get(nextKey).toCharArray());
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	};

	@Override
	public void beginTx() {
		System.out.println("Done!");
	}

	@Override
	public void commit() {
		writeToStore();
	}

	public void writeToStore() {
		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(storeFile)))) {
			oos.writeObject(map);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

}
