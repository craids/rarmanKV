package rarmanKV;

import java.security.SecureRandom;
import org.junit.jupiter.api.Test;
import core.SimpleKV;
import junit.framework.Assert;

class MemoryConstraintTest {
	private final float MEMORY_LIMIT_IN_MB = 500;

    @Test public void testInMemoryMapUse() {
    	System.out.println("Test for using more than 1GB RAM");
        SimpleKV kv = new SimpleKV();
        kv.beginTx();
        long beginMem = getMemoryFootprint();
        
        SecureRandom random = new SecureRandom();
        System.out.print("Begin writing...");

        // write 512MB to KV store
        for (int i = 0; i < 1000000; i++) {
            byte[] keyBytes = new byte[128];
            random.nextBytes(keyBytes);
            char[] chars = new String(keyBytes).toCharArray();  // 256 bytes (2byte/char, 128 chars)
        	kv.write(chars, chars); // write 512 bytes (key, value both 256)
        }
        
        System.out.println(" end writing");
        
        long endMem = getMemoryFootprint();
        long memDiff = (endMem - beginMem) / (1<<20);
        System.out.println("Footprint: " + memDiff + "MB");
        if (memDiff > MEMORY_LIMIT_IN_MB) {
            Assert.fail("Used too much RAM. KV test used " + memDiff + " MB of RAM, when limit was " + MEMORY_LIMIT_IN_MB);
        }
        kv.commit();
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
