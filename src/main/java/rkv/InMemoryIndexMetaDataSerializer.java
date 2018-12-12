
package rkv;

import java.nio.ByteBuffer;

class InMemoryIndexMetaDataSerializer implements HashTableValueSerializer<InMemoryIndexMetaData> {

	public void serialize(InMemoryIndexMetaData recordMetaData, ByteBuffer byteBuffer) {
		recordMetaData.serialize(byteBuffer);
		byteBuffer.flip();
	}

	public InMemoryIndexMetaData deserialize(ByteBuffer byteBuffer) {
		return InMemoryIndexMetaData.deserialize(byteBuffer);
	}

	public int serializedSize(InMemoryIndexMetaData recordMetaData) {
		return InMemoryIndexMetaData.SERIALIZED_SIZE;
	}
}
