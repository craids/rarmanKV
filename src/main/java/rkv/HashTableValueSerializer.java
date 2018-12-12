
// This code is a derivative work heavily modified from the OHC project. See NOTICE file for copyright and license.

package rkv;

import java.nio.ByteBuffer;

interface HashTableValueSerializer<T> {

	void serialize(T value, ByteBuffer buf);

	T deserialize(ByteBuffer buf);

	int serializedSize(T value);
}
