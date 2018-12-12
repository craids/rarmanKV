package core;

import java.util.stream.Stream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SimpleKV implements KeyValue {

	private final String storeFile = "rkv.dat";
	private final String logFile = "rkv.log";
	private boolean hasDirtyData = true;
	private HashMap<char[], char[]> map;

	@SuppressWarnings("unchecked")
	public SimpleKV() {
		map = new HashMap<>();
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
		if(map.containsKey(key))
			return map.get(key);
		else
			return readKV(key);
	}

	@Override
	public Iterator<KVPair> readRange(char[] startKey, char[] endKey) {
		return null;
		/*
		String start = new String(startKey);
		String end = new String(endKey);
		return new KVPairIterator(start, end);
		*/
	}

	@Override
	public void beginTx() {
		map.clear();
	}

	@Override
	public void commit() {
		for(Map.Entry<char[],char[]> es : map.entrySet())
			writeKV(es.getKey(), es.getValue());
		map.clear();
	}

	public void writeToStore() {
		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(storeFile)))) {
			oos.writeObject(map);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	private boolean writeKV(char[] key, char[] value)
	{
		try {
			FileWriter fw = new FileWriter(new File("rkv"+new String(key)));
			fw.write(value, 0, value.length);
			fw.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	private char[] readKV(char[] key)
	{
		try {
			String fname = "rkv"+new String(key);
			File f = new File(fname);
			if(f.exists())
				return new String(Files.readAllBytes(Paths.get(fname)), StandardCharsets.UTF_8).toCharArray();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private List<String> readLines(String filePath)
	{
	    List<String> lines = new ArrayList<String>();
	    try (Stream<String> stream = Files.lines( Paths.get(filePath), StandardCharsets.UTF_8))
	    {
	        stream.forEach(s -> lines.add(s));
	    }
	    catch (IOException e)
	    {
	        e.printStackTrace();
	    }
	    return lines;
	}
}
