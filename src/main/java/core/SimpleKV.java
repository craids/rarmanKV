package core;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.*;

import rkv.*;

public class SimpleKV implements KeyValue {

	private TrieMap<char[], char[]> map;
	private final String storeName = "rkv.dat";
	private boolean hasChanged = false;
	private ExecutorService executor;

	@SuppressWarnings("unchecked")
	public SimpleKV() {
		executor = Executors.newCachedThreadPool();
		File dir = new File(".");
		File [] files = dir.listFiles(new FilenameFilter() {
		    @Override
		    public boolean accept(File dir, String name) {
		        return name.startsWith("rkv-");
		    }
		});
		map = new TrieMap<char[], char[]>();
		ArrayList<Future> workload = new ArrayList<>();
		for (File f : files) {
		    try {
		    	Future fw = executor.submit(() -> map.put(f.getName().substring(4).toCharArray(), Files.readString(f.toPath()).toCharArray()));
		    	workload.add(fw);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		for(Future f : workload)
			while(!f.isDone());
	}

	@Override
	public SimpleKV initAndMakeStore(String path) {
		return new SimpleKV();
	}

	@Override
	public void write(char[] key, char[] value) {
		Future w1 = executor.submit(() -> map.put(key, value));
		Future w2 = executor.submit(new Runnable() {
			public void run() {
				try {
					FileWriter fw = new FileWriter(new File("rkv-".concat(new String(key))));
					fw.write(value);
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}});
		while(!w1.isDone() || !w2.isDone());
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
