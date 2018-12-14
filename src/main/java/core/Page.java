package core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

public class Page {
	boolean isDirty = false;
	int id;
	int numItems; // counter of # key/val pairs on this page
	int numBytes; // counter of # bytes on this page
	public Map<String, String> items = new HashMap<>();
	
	public Page(byte[] data, int id) throws IOException {
		numBytes = data.length;
		this.id = id;
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

		numItems = dis.readInt();
		for (int i = 0; i < numItems; i++) {
			int keyLen = dis.readInt();
			int valLen = dis.readInt();
			char[] key = new char[keyLen];
			char[] val = new char[valLen];
			
			for (int k = 0; k < keyLen; k++) key[k] = dis.readChar();
			for (int v = 0; v < valLen; v++) val[v] = dis.readChar();
			
			items.put(new String(key), new String(val));
		}
		dis.close();
	}
	
	// write or overwrite a key/val pair to this page
	public void write(String key, String val) {
		isDirty = true;
		if (items.containsKey(key)) {
			numItems--;
			String oldVal = items.get(key);
			 // assume 1 16bit string character = 1 char = 2 bytes
			numBytes -= key.length() * 2;
			numBytes -= oldVal.length() * 2;
		}
		numItems++;
		items.put(key, val);
		numBytes += key.length() * 2;
		numBytes += val.length() * 2;
	}
	
	public boolean hasSpace() {
		if ((SimpleKV.PAGE_SIZE - numBytes) < SimpleKV.PAGE_PADDING) return true;
		return false;
	}
	
	// get byte-ified data of this page to be written to disk
	public byte[] getPageData() {
		byte[] pLen = null;
		byte[] kLen = null;
		byte[] vLen = null;
 		int len = SimpleKV.PAGE_SIZE;
		int offset = 0;
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
		ByteBuffer b = ByteBuffer.allocate(4);
		
		//write numValues as bytes
		b.putInt(numItems);
		pLen = b.array();
		baos.write(pLen, offset, pLen.length);
		offset += pLen.length;

		// write the # items on the page
		// write each key/value pair on the page, in the form:
		// [key len, # chars] [val len, # chars] [key] [val]
		
		for(String k : items.keySet()) {
			int kL = k.length();
			ByteBuffer kB = ByteBuffer.allocate(4);
			kLen = kB.array();
			String v = items.get(k);
			int iL = v.length();
			ByteBuffer vB = ByteBuffer.allocate(4);
			vLen = vB.array();
			
			baos.write(kLen, offset, kLen.length);
			System.out.println("Wrote key length " + kLen + " at offset " + offset);
			offset += kLen.length;
			baos.write(k.getBytes(), offset, k.getBytes().length);
			System.out.println("Wrote key " + k + " at offset " + offset);
			offset += k.getBytes().length;
			
			baos.write(vLen, offset, vLen.length);
			System.out.println("Wrote value length " + vLen + " at offset " + offset);
			offset += vLen.length;
			baos.write(v.getBytes(), offset, v.getBytes().length);
			System.out.println("Wrote value " + k + " at offset " + offset);
			offset += v.getBytes().length;
			
		}
		
		return baos.toByteArray();
	}
	
	
	// flush this page to disk
	public void flush() throws IOException {
        RandomAccessFile r = new RandomAccessFile(SimpleKV.file, "rw");
        final int offset = id * SimpleKV.PAGE_SIZE;
        final byte[] data = getPageData();
        
        r.seek(offset);
        r.write(data);
        r.close();
	}
}