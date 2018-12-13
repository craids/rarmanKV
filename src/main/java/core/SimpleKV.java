package core;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.concurrent.*;

import rkv.*;

public class SimpleKV implements KeyValue {

	private TrieMap<char[], char[]> map;
	private final String storeName = "rkv.dat";
	private boolean hasChanged = false;
	private ExecutorService executor;
	private ObjectOutputStream oos;

	@SuppressWarnings("unchecked")
	public SimpleKV() {
		executor = Executors.newCachedThreadPool();
		map = new TrieMap<char[], char[]>();
		try {
		FileInputStream fis = new FileInputStream(new File(storeName));
		ObjectInputStream ois = new ObjectInputStream(fis);
		Object o = null;
		
		while((o = ois.readObject()) != null)
		{
			KVPair kp = (KVPair)o;
			map.put(kp.element1, kp.element2);
		}
		ois.close();
		fis.close();
		
		FileOutputStream fos = new FileOutputStream(new File(storeName), true);
		oos = new ObjectOutputStream(fos);
		
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		//for(Future f : workload)
		//	while(!f.isDone());
	}

	@Override
	public SimpleKV initAndMakeStore(String path) {
		return new SimpleKV();
	}

	@Override
	public void write(char[] key, char[] value) {
		map.put(key, value);
		try {
		oos.writeObject(new KVPair(key, value));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
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

	}
}
