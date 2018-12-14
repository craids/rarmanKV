package rarmanKV;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;

import org.junit.jupiter.api.*;

import core.Page;
import core.SimpleKV;

class BasicFunctionalityTest {
	private SimpleKV kv = new SimpleKV();
	
	@BeforeEach
	void setUp() {
		kv = kv.initAndMakeStore("test.out");
	}
	
	@AfterEach
	void cleanUp() {
		kv.reset();
	}
	
	@Test
	void testPageReadWrite() {
		try {
			Page p = new Page(new byte[SimpleKV.PAGE_SIZE], 1);
			p.write("a", "a");
			p.write("b", "b");
			p.write("c", "c");
			
			Page p2 = new Page(p.serializeData(), 2);
			int dummy = 0;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
