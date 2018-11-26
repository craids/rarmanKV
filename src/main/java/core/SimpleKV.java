package core;

import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

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
		//return map.get(new Symbol(key)).asArray();
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
    	//private Symbol endKey, currentKey;
    	//private  Iterator<Symbol> ksIterator;
    	private Iterator<Map.Entry<Symbol, Symbol>> ksIterator;
    	
        public KVPairIterator(Symbol start, Symbol end)
        {
        	ksIterator = map.subMap(start, true, end, true).entrySet().iterator();
        	//currentKey = map.ceilingKey(start);
        	//endKey = map.floorKey(end);
        }
        
        @Override
        public boolean hasNext() {
            return ksIterator.hasNext();
        	//return !currentKey.equals(endKey) && currentKey != null && endKey != null;
        }

        @Override
        public KVPair next() {
            Map.Entry<Symbol, Symbol> next = ksIterator.next();
        	//currentKey = map.higherKey(currentKey);
            return new KVPair(next.getKey().asArray(), next.getValue().asArray());
            /*
        	currentKey = map.higherKey(currentKey);
        	if(currentKey != null)
        		return new KVPair(currentKey.asArray(), map.get(currentKey).asArray());
        	return null;*/
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
