
package rkv;

import java.io.*;

public interface Serializer<A> {

	public void serialize(DataOutput out, A obj) throws IOException;

	public A deserialize(DataInput in) throws IOException, ClassNotFoundException;

}
