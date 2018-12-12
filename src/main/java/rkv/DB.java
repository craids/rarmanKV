package rkv;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;

public interface DB {

	void close();

	boolean isClosed();

	void clearCache();

	void defrag(boolean sortCollections);

	void commit();

	void rollback();

	String calculateStatistics();

	void copyToZip(String zipFile);

	<K, V> ConcurrentMap<K, V> getHashMap(String name);

	<K, V> ConcurrentMap<K, V> createHashMap(String name);

	<K, V> ConcurrentMap<K, V> createHashMap(String name, Serializer<K> keySerializer, Serializer<V> valueSerializer);

	<K> Set<K> createHashSet(String name);

	<K> Set<K> getHashSet(String name);

	<K> Set<K> createHashSet(String name, Serializer<K> keySerializer);

	<K, V> ConcurrentNavigableMap<K, V> getTreeMap(String name);

	<K extends Comparable, V> NavigableMap<K, V> createTreeMap(String name);

	<K, V> ConcurrentNavigableMap<K, V> createTreeMap(String name, Comparator<K> keyComparator,
			Serializer<K> keySerializer, Serializer<V> valueSerializer);

	<K> NavigableSet<K> getTreeSet(String name);

	<K> NavigableSet<K> createTreeSet(String name);

	<K> NavigableSet<K> createTreeSet(String name, Comparator<K> keyComparator, Serializer<K> keySerializer);

	<K> List<K> createLinkedList(String name);

	<K> List<K> createLinkedList(String name, Serializer<K> serializer);

	<K> List<K> getLinkedList(String name);

	Map<String, Object> getCollections();

	void deleteCollection(String name);

	long collectionSize(Object collection);

}
