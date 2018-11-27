
import org.junit.Before;
import static org.junit.jupiter.api.Assertions.*;
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

}
