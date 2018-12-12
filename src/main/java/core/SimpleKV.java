package core;

import rkv.*;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

public class SimpleKV implements KeyValue {

	private final String dbFileName = "rkv.dat";
	private final String collectionName = "rkvstore";
	private final ConcurrentMap<char[], char[]> map;
	private final DB db;
	
	public SimpleKV() {
		db = DBMaker.openFile(dbFileName).make();
		map = db.createHashMap(collectionName);
	}

	@Override
	public SimpleKV initAndMakeStore(String path) {
		return new SimpleKV();
	}

	@Override
	public void write(char[] key, char[] value) {
		map.put(key, value);
	}

	@Override
	public char[] read(char[] key) {
		return map.get(key);
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
		db.commit();
	}
}
