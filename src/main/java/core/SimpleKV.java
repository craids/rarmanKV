package core;

import java.io.*;
import java.nio.file.Files;
import java.util.Iterator;

public class SimpleKV implements KeyValue {

	private Trie<String> pt;
	private final String storeName = "rkv.dat";
	private boolean hasChanged = false;

	@SuppressWarnings("unchecked")
	public SimpleKV() {
		File dir = new File(".");
		File [] files = dir.listFiles(new FilenameFilter() {
		    @Override
		    public boolean accept(File dir, String name) {
		        return name.startsWith("rkv-");
		    }
		});
		pt = new Trie<String>();
		for (File f : files) {
		    try {
				pt.put(f.getName().substring(4), new String(Files.readAllBytes(f.toPath())));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public SimpleKV initAndMakeStore(String path) {
		return new SimpleKV();
	}

	@Override
	public void write(char[] key, char[] value) {
		pt.put(new String(key), new String(value));
				try {
					FileWriter fw = new FileWriter(new File("rkv-".concat(new String(key))));
					fw.write(value);
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
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

	}
}
