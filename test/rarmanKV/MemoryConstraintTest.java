package rarmanKV;

import java.security.SecureRandom;
import org.junit.jupiter.api.Test;
import core.SimpleKV;
import junit.framework.Assert;

class MemoryConstraintTest {
	private final float MEMORY_LIMIT_IN_MB = 500;

    @Test public void testInMemoryMapUse() {
    	System.out.println("Test for using more than 500MB RAM");
        SimpleKV kv = new SimpleKV();
        kv.reset(); // empty out kv store before testing
		kv = kv.initAndMakeStore("test.out");
        kv.beginTx();
        long beginMem = getMemoryFootprint();
        
        SecureRandom random = new SecureRandom();
        System.out.print("Begin writing...");

        try {
            // write 1GB to KV store
            for (int i = 0; i < 1000000; i++) {
            	if (i == 250000) System.out.print("25%...");
            	if (i == 500000) System.out.print("50%...");
            	if (i == 750000) System.out.print("75%...");
                byte[] keyBytes = new byte[256];
                random.nextBytes(keyBytes);
                char[] chars = new String(keyBytes).toCharArray();  // 512 bytes (2byte/char, 256 chars)
            	kv.write(chars, chars); // write 1024 bytes (key, value both 512)
            }
        } catch (Exception e) {
        	e.printStackTrace();
        	kv.reset();
        	Assert.fail("Error while writing to db");
        }
        
        System.out.println(" end writing");
        long endMem = getMemoryFootprint();
        long memDiff = (endMem - beginMem) / (1<<20);
        System.out.println("Footprint: " + memDiff + "MB");
        
        kv.commit();
        kv.reset();
        
        if (memDiff > MEMORY_LIMIT_IN_MB) {
            Assert.fail("Used too much RAM. KV test used " + memDiff + " MB of RAM, when limit was " + MEMORY_LIMIT_IN_MB);
        }
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
