package rkv;

import java.util.AbstractSet;
import java.util.Iterator;

class HTreeSet<E> extends AbstractSet<E> {

	final HTree<E, Object> map;

	HTreeSet(HTree map) {
		this.map = map;
	}

	public Iterator<E> iterator() {
		return map.keySet().iterator();
	}

	public int size() {
		return map.size();
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public boolean contains(Object o) {
		return map.containsKey(o);
	}

	public boolean add(E e) {
		return map.put(e, Utils.EMPTY_STRING) == null;
	}

	public boolean remove(Object o) {
		return map.remove(o) == Utils.EMPTY_STRING;
	}

	public void clear() {
		map.clear();
	}

}
