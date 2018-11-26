package core;

import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class SimpleKV implements KeyValue {
	
	private static final ExecutorService es = Executors.newCachedThreadPool();
	private final ConcurrentSkipListMap<Symbol,Symbol> map;
	private final ArrayList<Symbol> keys;
	
    public SimpleKV() {
    	this.map = new ConcurrentSkipListMap<>();
    	this.keys = new ArrayList<>();
    }

    @Override
    public SimpleKV initAndMakeStore(String path) {
    	return new SimpleKV();
    }

    @Override
    public void write(char[] key, char[] value) {
    	// System.out.println("Written!");
    	es.submit(() -> {
	    	Symbol k = new Symbol(key);
	    	map.put(k, new Symbol(value));
	    	keys.add(k);
	    	Collections.sort(keys);
    	});
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
    	//private  Symbol startKey, endKey, currentKey;
    	//private  Iterator<Symbol> ksIterator;
    	private Iterator<Map.Entry<Symbol, Symbol>> ksIterator;
    	
        public KVPairIterator(Symbol start, Symbol end)
        {
        	ksIterator = map.subMap(start, true, end, true).entrySet().iterator();
        	/*startKey = map.ceilingKey(start);
        	endKey = map.floorKey(end);
        	currentKey = startKey;*/
        }
        
        @Override
        public boolean hasNext() {
            return ksIterator.hasNext();
        	//return startKey != null && endKey != null && !currentKey.equals(endKey);
        }

        @Override
        public KVPair next() {
            Map.Entry<Symbol, Symbol> next = ksIterator.next();
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
