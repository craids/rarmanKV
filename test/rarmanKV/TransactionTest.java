package rarmanKV;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;
import core.SimpleKV;

class TransactionTest {
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
	void testOverwrite() {
		assertNotNull(kv, "kv store is null!");
		kv.beginTx();
		char[] key = "hi".toCharArray();
		char[] val = "bye".toCharArray();
		kv.write(key, val);
		kv.commit();
		kv.crash(); // simulate crash
		kv = kv.initAndMakeStore("test.out");
		char[] actual = kv.read(key);
		assertArrayEquals(val, actual);
	}
}
