

import java.security.SecureRandom;
import java.util.Iterator;

import org.junit.jupiter.api.Test;

import core.KVPair;
import core.SimpleKV;
import core.Symbol;
import junit.framework.Assert;

class MemoryConstraintTest {
	private final float MEMORY_LIMIT_IN_MB = 1000;

    @Test public void testInMemoryMapUse() {
    	System.out.println("Test for using more than 1GB RAM");
        SimpleKV kv = new SimpleKV();
        long beginMem = getMemoryFootprint();
        Symbol max = null;
        Symbol min = null;
        char[] maxA = new char[128];
        char[] minA = new char[128];
        
        SecureRandom random = new SecureRandom();
        System.out.print("Begin writing...");

        // write 512MB to KV store
        for (int i = 0; i < 1000000; i++) {
            byte[] keyBytes = new byte[128];
            random.nextBytes(keyBytes);
            char[] chars = new String(keyBytes).toCharArray();  // 256 bytes (2byte/char, 128 chars)
            Symbol key = new Symbol(new String(chars));
            if(max == null) {
            	max = key;
            	maxA = chars;
            } else if (key.compareTo(max) > 0) {
            	max = key;
            	maxA = chars;
            }
            if (min == null) {
            	min = key;
            	minA = chars;
            } else if (key.compareTo(min) < 0) {
            	min = key;
            	minA = chars;
            }
            
        	kv.write(chars, chars); // write 512 bytes (key, value both 256)
        }
        
        System.out.println(" end writing");
        
        long endMem = getMemoryFootprint();
        long memDiff = (endMem - beginMem) / (1<<20);
        System.out.println("Footprint: " + memDiff + "MB");
        if (memDiff > MEMORY_LIMIT_IN_MB) {
            Assert.fail("Used too much RAM. KV test used " + memDiff + " MB of RAM, when limit was " + MEMORY_LIMIT_IN_MB);
        }
        
        Assert.assertNotNull(max);
        Assert.assertNotNull(min);
        
        long start = System.currentTimeMillis();
        Iterator<KVPair> range = kv.readRange(minA, maxA);
        while(range.hasNext()) {
        	range.next();
        }
        long end = System.currentTimeMillis();
        System.out.println("ReadRange took " + (end - start));
       
    }

    /**
     * This method taken from simpledb.SystemTestUtil file
     * 
     * Returns number of bytes of RAM used by JVM after calling System.gc many times.
     * @return amount of RAM (in bytes) used by JVM
     */
    private long getMemoryFootprint() {
        // Call System.gc in a loop until it stops freeing memory. This is
        // still no guarantee that all the memory is freed, since System.gc is
        // just a "hint".
        Runtime runtime = Runtime.getRuntime();
        long memAfter = runtime.totalMemory() - runtime.freeMemory();
        long memBefore = memAfter + 1;
        while (memBefore != memAfter) {
            memBefore = memAfter;
            System.gc();
            memAfter = runtime.totalMemory() - runtime.freeMemory();
        }
        return memAfter;
    }
}
