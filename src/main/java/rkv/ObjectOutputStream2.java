package rkv;

import java.io.*;
import java.util.ArrayList;

import rkv.SerialClassInfo.ClassInfo;

public class ObjectOutputStream2 extends DataOutputStream implements ObjectOutput {

	public ObjectOutputStream2(OutputStream out) {
		super(out);
	}

	public void writeObject(Object obj) throws IOException {
		ArrayList registered = new ArrayList();
		Serialization ser = new Serialization(null, 0, registered);

		byte[] data = ser.serialize(obj);
		// write class info first
		SerialClassInfo.serializer.serialize(this, registered);
		// and write data
		write(data);
	}
}
