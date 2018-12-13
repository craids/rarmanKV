package core;

import java.io.*;
import java.util.Iterator;

public class SimpleKV implements KeyValue {

	private Trie<String> pt;
	private final String storeName = "rkv.dat";

	@SuppressWarnings("unchecked")
	public SimpleKV() {
		File f = new File(storeName);
		if (f.exists()) {
			try {
				FileInputStream fis = new FileInputStream(storeName);
				ObjectInputStream ois = new ObjectInputStream(fis);
				pt = (Trie<String>) ois.readObject();
				ois.close();
				fis.close();
				return;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		pt = new Trie<String>();
	}

	@Override
	public SimpleKV initAndMakeStore(String path) {
		return new SimpleKV();
	}

	@Override
	public void write(char[] key, char[] value) {
		pt.put(new String(key), new String(value));
	}

	@Override
	public char[] read(char[] key) {
		return pt.get(new String(key)).toCharArray();
	}

	@Override
	public Iterator<KVPair> readRange(char[] startKey, char[] endKey) {
		return null;
	}

	@Override
	public void beginTx() {
		// do nothing
	}

	@Override
	public void commit() {
		try {
			FileOutputStream fos = new FileOutputStream(storeName);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(pt);
			oos.close();
			fos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
