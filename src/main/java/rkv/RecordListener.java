
package rkv;

import java.io.IOException;

interface RecordListener<K, V> {

	void recordInserted(K key, V value) throws IOException;

	void recordUpdated(K key, V oldValue, V newValue) throws IOException;

	void recordRemoved(K key, V value) throws IOException;

}
