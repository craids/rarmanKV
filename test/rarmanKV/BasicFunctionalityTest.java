package rarmanKV;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import core.KVPair;
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
		
		for (int i = 0; i < 100000; i++) {
			char[] keyChars = new char[16];
			for (int k=0; k < 16; k++) keyChars[k] = getRandomCharacter();
        	kv.write(keyChars, keyChars);
        	written.add(keyChars);
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
	
	@Test
	void rangeTest() {
		char[] k1 = "a".toCharArray();
		char[] k2 = "b".toCharArray();
		char[] k3 = "c".toCharArray();
		char[] k4 = "d".toCharArray();
		char[] k5 = "e".toCharArray();
		char[] k6 = "f".toCharArray();
		char[] k7 = "g".toCharArray();
		char[] k8 = "h".toCharArray();
		char[] k9 = "i".toCharArray();
		char[] k10 = "j".toCharArray();
		char[] v = "a".toCharArray();
		kv.write(k1, v);
		kv.write(k2, v);
		kv.write(k3, v);
		kv.write(k4, v);
		kv.write(k5, v);
		kv.write(k6, v);
		kv.write(k7, v);
		kv.write(k8, v);
		kv.write(k9, v);
		kv.write(k10, v);
		
		int count = 0;
		Iterator<KVPair> it = kv.readRange("a".toCharArray(), "z".toCharArray());
		while(it.hasNext()) {
			KVPair dispose = it.next();
			count++;
		}
		assertEquals(count, 10);
		count = 0;
		
		it = kv.readRange("b".toCharArray(), "j".toCharArray());
		while(it.hasNext()) {
			KVPair dispose = it.next();
			count++;
		}
		assertEquals(count, 9);
		count = 0;
		
		it = kv.readRange("c".toCharArray(), "e".toCharArray());
		while(it.hasNext()) {
			KVPair dispose = it.next();
			count++;
		}
		assertEquals(count, 3);
	}
	
    private static char getRandomCharacter() {
        Random r = new Random();
        return (char)(r.nextInt(95)+32);
    }
}
