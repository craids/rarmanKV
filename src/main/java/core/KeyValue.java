package core;

import java.util.Iterator;

public interface KeyValue {

	SimpleKV initAndMakeStore(String path);

	void write(char[] key, char[] value);

	char[] read(char[] key);

	Iterator<KVPair> readRange(char[] startKey, char[] endKey);

	void beginTx();

	public void commit();

}
