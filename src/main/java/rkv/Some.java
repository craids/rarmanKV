package rkv;

public class Some<V> extends Option<V> {
	final V value;

	public Some(V v) {
		value = v;
	}

	public V get() {
		return value;
	}

	public boolean nonEmpty() {
		return value != null;
	}
}
