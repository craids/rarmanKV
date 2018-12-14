package core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

public class SimpleKV implements KeyValue {
	public static final int MEMORY_LIMIT = 500000000; // in bytes, slightly under 500MB for safety
	public static final int PAGE_SIZE = 4096; // page size in bytes
	public static final int PAGE_PADDING = 300; // extra space in pages for safety
	public static int lastPageId = 0; // used to generate new page ids
	public static File file = new File("test.out");
	
	private TreeMap<String,String> map;
	private HashMap<Integer, Page> pageMap;
	private HashSet<Page> dirtyPages;
	
    public SimpleKV() {
    	this.map = new TreeMap<>();
    	this.pageMap = new HashMap<>();
    	this.dirtyPages = new HashSet<>();
    }

    @Override
    public SimpleKV initAndMakeStore(String path) {
    	file = new File(path);
    	return new SimpleKV();
    }

    @Override
    public void write(char[] key, char[] value) {
    	String keyString = new String(key);
    	String valueString = new String(value);
    	
    	// see if our key is already on one of the pages in cache
    	for (Page p: pageMap.values()) {
    		if (p.items.containsKey(keyString)) {
    			p.write(keyString, valueString);
    			dirtyPages.add(p);
    			return;
    		}
    	}
    	
    	// need to see if key already exists on disk (this will be slow af)
    	for (int i = 0; i < lastPageId; i++) {
    		if (!pageMap.keySet().contains(i)) { // make sure we didn't just look at this page in cache
    			Page p = addPageToMemory(i);
    			if (p.items.containsKey(keyString)) p.write(keyString, valueString);
    			dirtyPages.add(p);
    			return;
    		}
    	}
    	
    	// if we got here, the key doesn't exist yet in our db. write it to last page, if it has room; otherwise,
    	// make a new page.
    	Page lastPage = pageMap.keySet().contains(lastPageId) ? 
    					pageMap.get(lastPageId) : 
    					addPageToMemory(lastPageId);
    	if (lastPage.hasSpace()) {
    		lastPage.write(keyString, valueString);
    		dirtyPages.add(lastPage);
    	} else {
    		Page p = addPageToMemory(lastPageId++);
    		p.write(keyString, valueString);
    		dirtyPages.add(p);
    	}
    	
    }

    @Override
    public char[] read(char[] key) {
    	String keyString = new String(key);
    	
    	// try reading from cache
    	for (Page p: pageMap.values()) {
    		if (p.items.containsKey(keyString)) {
    			return p.items.get(keyString).toCharArray();
    		}
    	}
    	
    	// read from disk until found
    	for (int i = 0; i < lastPageId; i++) {
    		if (!pageMap.keySet().contains(i)) {
    			Page p = addPageToMemory(i);
    			if (p.items.containsKey(keyString)) return p.items.get(keyString).toCharArray();
    		}
    	}
    	
    	return null;
    }
    
    // pull a page into main memory
    public Page addPageToMemory(int pageNo)  {
    	try {
			checkAndEvictPage();
			RandomAccessFile r = new RandomAccessFile(file, "r");
	        byte[] b = new byte[PAGE_SIZE];
	        
	        r.seek(pageNo * PAGE_SIZE);
	        r.read(b);
	        r.close();
	        
	        Page p = new Page(b, pageNo);
        	pageMap.put(pageNo, p);
        	return p;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
    }
    
    // evict a page, if necessary
    public void checkAndEvictPage() throws Exception {
    	if (pageMap.size() * PAGE_SIZE > MEMORY_LIMIT) { // only evict if pages will exceed MEMORY_LIMIT
    		Page[] pages = (Page[]) pageMap.values().toArray();
    		int i = 0;
    		while (i < pageMap.size() - 1 && pages[i].isDirty) i++; // only evict clean
    		Page toEvict = pages[i];
    		if (toEvict.isDirty) throw new Exception("no clean pages to evict");
    		pageMap.remove(toEvict.id);
    	}
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
    }

    @Override
    public void commit() {
    	// flush each dirty page
    	for (Page p: dirtyPages) {
    		try {
				p.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	// clear out dirtyPages
    	dirtyPages = new HashSet<>();
    }

}
