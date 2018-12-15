package core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

public class SimpleKV implements KeyValue {
	public static final int MEMORY_LIMIT = 500000000; // in bytes, slightly under 500MB for safety
	public static final int PAGE_SIZE = 10000000; // page size in bytes
	public static final int PAGE_PADDING = 1000; // extra space in pages for safety
	public static int lastPageId = 0; // used to generate new page ids
	public static File file;
	
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
    	try {
			file.createNewFile();
		} catch (IOException e) {
			throw new RuntimeException("Failed to make file: " + e.getMessage());
		}
    	return new SimpleKV();
    }
    
    // WARNING!!! use only in testing, will wipe the DB file
    public void reset() {
    	if (file != null) file.delete();
    	lastPageId = 0;
    }
    
    // clears main memory. used in testing to simulate a crash
    public void crash() {
    	pageMap = new HashMap<>();
    	lastPageId = 0;
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
    			if (p.items.containsKey(keyString)) {
    				p.write(keyString, valueString);
    				dirtyPages.add(p);
    				return;
    			}
    		}
    	}
    	
    	// if we got here, the key doesn't exist yet in our db. write it to last page, if it has room; otherwise,
    	// make a new page.
    	Page lastPage = pageMap.keySet().contains(lastPageId) ? 
    					pageMap.get(lastPageId) : 
    					addPageToMemory(lastPageId);

    	if (lastPage.hasSpace(key.length + value.length)) {
    		lastPage.write(keyString, valueString);
    		dirtyPages.add(lastPage);
    	} else {
    		lastPageId++;
    		Page p = addPageToMemory(lastPageId);
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
    	for (int i = 0; i <= lastPageId; i++) {
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
    	if ((pageMap.size() + 1) * PAGE_SIZE > MEMORY_LIMIT) { // only evict if pages will exceed MEMORY_LIMIT
//    		Object[] pages = pageMap.values().toArray();
//    		int i = 0;
//    		while (i < pageMap.size() - 1 && ((Page) pages[i]).isDirty) i++; // only evict clean
//    		Page toEvict = (Page) pages[i];
//    		if (toEvict.isDirty) { // no clean pages to evict
//    			flushDirtyPages();
//    			return;
//    		}
//    		pageMap.remove(toEvict.id);
    		flushDirtyPages();
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
    
    public void flushDirtyPages() {
    	// flush each dirty page
    	for (Page p: dirtyPages) {
    		try {
				p.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	// clear out in-memory set of dirtyPages
    	dirtyPages = new HashSet<>();
    }

    @Override
    public void commit() {
    	flushDirtyPages();
    }

}
