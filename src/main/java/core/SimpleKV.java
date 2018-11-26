package core;

import java.util.TreeMap;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.Iterator;

public class SimpleKV implements KeyValue {
	
	private final TreeMap<Symbol,Symbol> map;
	
    public SimpleKV() {
    	this.map = new TreeMap<>();
    }

    @Override
    public SimpleKV initAndMakeStore(String path) {
    	return new SimpleKV();
    }

    @Override
    public void write(char[] key, char[] value) {
    	// System.out.println("Written!");
    	map.put(new Symbol(key), new Symbol(value));
    }

    @Override
    public char[] read(char[] key) {
		// System.out.println("Read!");
		return map.get(new Symbol(key)).asArray();
    }

    @Override
    public Iterator<KVPair> readRange(char[] startKey, char[] endKey) {
		// System.out.println("Read range!");
		Symbol start = new Symbol(startKey);
		Symbol end = new Symbol(endKey);
		return new KVPairIterator(start, end);
    }
    
    private class KVPairIterator implements Iterator<KVPair> {
    	private Symbol startKey, endKey, currentKey;
    	
        public KVPairIterator(Symbol start, Symbol end)
        {
        	//ksIterator = map.navigableKeySet().subSet(start, true, end, true).iterator();
        	startKey = map.ceilingKey(start);
        	endKey = map.floorKey(end);
        	currentKey = startKey;
        }
        
        @Override
        public boolean hasNext() {
            //return ksIterator.hasNext();
        	return startKey != null && endKey != null && !currentKey.equals(endKey);
        }

        @Override
        public KVPair next() {
            //String nextKey = ksIterator.next();
            //return new KVPair(nextKey.toCharArray(), map.get(nextKey).toCharArray());
        	currentKey = map.higherKey(currentKey);
        	if(currentKey != null)
        		return new KVPair(currentKey.asArray(), map.get(currentKey).asArray());
        	return null;
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
	System.out.println("Done!");
    }

}
