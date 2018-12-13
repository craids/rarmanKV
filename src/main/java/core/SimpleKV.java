package core;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.*;

import rkv.*;

public class SimpleKV implements KeyValue {

	private MRUMap<String, String> map;
	private final String storeName = "rkv.dat";
	private boolean hasChanged = false;
	private ExecutorService executor;
	private ObjectOutputStream oos;

	@SuppressWarnings("unchecked")
	public SimpleKV() {
		map = new MRUMap<String, String>(2048);
	}

	@Override
	public SimpleKV initAndMakeStore(String path) {
		return new SimpleKV();
	}

	@Override
	public void write(char[] key, char[] value) {
		try{
			String skey = new String(key);
			BufferedWriter bw = Files.newBufferedWriter(new File(new String(key)).toPath());
			bw.write(value);
			map.put(skey, new String(value));
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	@Override
	public char[] read(char[] key) {
		String skey = new String(key);
		if(map.containsKey(skey))
		{
			return map.get(skey).toCharArray();
		}
		try
		{
			String s = Files.readString(new File(new String(key)).toPath());
			map.put(skey, s);
			return s.toCharArray();
		}
		catch(Exception ex) {
			return null;
		}
		//return map.get(key);
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
