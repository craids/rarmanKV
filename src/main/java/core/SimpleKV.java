package core;

import java.util.TreeMap;
import java.util.Iterator;

public class SimpleKV implements KeyValue {
	
	private TreeMap<String,String> map;
	
    public SimpleKV() {
    	this.map = new TreeMap<>();
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
		String skey = new String(key);
		//if(map.containsKey(skey))
		return map.get(new String(key)).toCharArray();
		//return null;
    }

    @Override
    public Iterator<KVPair> readRange(char[] startKey, char[] endKey) {
		String start = new String(startKey);
		String end = new String(endKey);
		return new KVPairIterator(start,end);
    }
    
    private class KVPairIterator implements Iterator<KVPair> {
    	private Iterator<String> ksIterator;
    	
        public KVPairIterator(String start,String end)
        {
        	ksIterator = map.navigableKeySet().subSet(start, true, end, true).iterator();
        }
        
        @Override
        public boolean hasNext() {
            return ksIterator.hasNext();
        }

        @Override
        public KVPair next() {
            String nextKey = ksIterator.next();
            return new KVPair(nextKey.toCharArray(), map.get(nextKey).toCharArray());
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
