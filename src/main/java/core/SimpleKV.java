package core;

import rkv.*;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import net.openhft.chronicle.map.*;

public class SimpleKV implements KeyValue {
	
	private RKVDB db;
	private ChronicleMap<String, String> map;
	
	public SimpleKV() {
		ChronicleMapBuilder<String, String> mapBuilder =
			    ChronicleMapBuilder.of(String.class, String.class)
			        .name("rkvdb")
			        .entries(500_000);
			try {
				map =
				    mapBuilder.createOrRecoverPersistedTo(new File("rkv.db"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
		return null;
	}

	@Override
	public void beginTx() {
		// do nothing
	}

	@Override
	public void commit() {
		
	}
}
