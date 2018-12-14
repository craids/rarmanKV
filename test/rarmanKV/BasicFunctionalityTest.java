package rarmanKV;

import static org.junit.jupiter.api.Assertions.*;
import java.security.SecureRandom;
import java.util.*;

import org.junit.jupiter.api.*;
import core.SimpleKV;

class BasicFunctionalityTest {
	private SimpleKV kv = new SimpleKV();
	
	@BeforeEach
	void setUp() {
		kv.reset(); // empty out kv store before testing
		kv = kv.initAndMakeStore("test.out");
	}

	@Test
	void testTinyReadWrite() {
		assertNotNull(kv, "kv store is null!");
		kv.beginTx();
		char[] key = "hi".toCharArray();
		char[] val = "bye".toCharArray();
		kv.write(key, val);
		char[] actual = kv.read(key);
		assertArrayEquals(val, actual);
		kv.commit();
	}
	
	@Test
	void testBigReadWrite() {
		assertNotNull(kv, "kv store is null!");
		kv.beginTx();
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
		kv.commit();
	}
	
	@Test
	void testOverwrite() {
		assertNotNull(kv, "kv store is null!");
		kv.beginTx();
		char[] key = "hi".toCharArray();
		char[] val1 = "bye".toCharArray();
		char[] val2 = "hello".toCharArray();
		kv.write(key, val1);
		char[] actual1 = kv.read(key);
		assertArrayEquals(val1, actual1);
		kv.write(key, val2);
		char[] actual2 = kv.read(key);
		assertArrayEquals(val2, actual2);
		kv.commit();
	}
}
