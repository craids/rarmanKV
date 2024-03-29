package rarmanKV;

import org.junit.Before;
import static org.junit.jupiter.api.Assertions.*;
import java.security.SecureRandom;
import java.util.*;
import org.junit.jupiter.api.Test;
import core.SimpleKV;

class BasicFunctionalityTest {
	private SimpleKV kv = new SimpleKV();
	
	@Before
	void setUp() {
		kv = kv.initAndMakeStore(""); // empty out kv store before testing
	}

	@Test
	void testTinyReadWrite() {
		char[] key = "hi".toCharArray();
		char[] val = "bye".toCharArray();
		kv.write(key, val);
		char[] actual = kv.read(key);
		assertArrayEquals(val, actual);
	}
	
	@Test
	void testBigReadWrite() {
		Set<char[]> written = new HashSet<>();
		SecureRandom random = new SecureRandom();
		
		for (int i = 0; i < 100000; i++) {
			byte[] keyBytes = new byte[16];
            random.nextBytes(keyBytes);
            char[] chars = new String(keyBytes).toCharArray();
        	kv.write(chars, chars);
        	written.add(chars);
		}
		
		for (char[] chars : written) {
			char[] actual = kv.read(chars);
			assertArrayEquals(chars, actual);
		}
	}
}
