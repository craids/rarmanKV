package core;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Trie<V> implements Serializable {

	private static final long serialVersionUID = 1L;
	Entry<V> entry;
	char key;
	Map<Character, Trie<V>> childrens;

	public Trie() {
		this.childrens = new HashMap<Character, Trie<V>>(10);
		entry = new Entry<V>();
	}

	/** non-public, used by _put() */
	Trie(char key) {
		this.childrens = new HashMap<Character, Trie<V>>(10);
		this.key = key;
		entry = new Entry<V>();
	}

	public void put(String key, V value) {
		_put(new StringBuffer(key), new StringBuffer(""), value);
	}

	void _put(StringBuffer remainder, StringBuffer prefix, V value) {
		if (remainder.length() > 0) {
			char keyElement = remainder.charAt(0);
			Trie<V> t = null;
			try {
				t = childrens.get(keyElement);
			} catch (IndexOutOfBoundsException e) {
			}
			if (t == null) {
				t = new Trie<V>(keyElement);
				childrens.put(keyElement, t);
			}
			prefix.append(remainder.charAt(0));
			t._put(remainder.deleteCharAt(0), prefix, value);
		} else {
			this.entry.value = value;
			this.entry.prefix = prefix.toString();
		}

	}

	/**
	 * Retrieves element from prefix table matching as a prefix to provided key.
	 * E.g. is key is "abcde" and prefix table has node "ab" then this call will
	 * return "ab"
	 * 
	 * @param key a string which starts with prefix to be searched in the table
	 *            (e.g. phone number)
	 * @return an Object assosiated with matching prefix (i.e if key is a phone
	 *         number it may return a corresponding country name)
	 */
	public V get(String key) {
		return _get(new StringBuffer(key), 0);
	}

	/**
	 * Returns true if key has matching prefix in the table
	 */
	public boolean hasPrefix(String key) {
		return ((this.get(key) != null) ? true : false);
	}

	V _get(StringBuffer key, int level) {
		if (key.length() > 0) {
			Trie<V> t = childrens.get(key.charAt(0));
			if (t != null) {
				return t._get(key.deleteCharAt(0), ++level);
			} else {
				return (level > 0) ? entry.value : null;
			}

		} else {
			return entry.value;
		}
	}

	@Override
	public String toString() {
		return "Trie [entry=" + entry + ", key=" + key + ", childrens=" + childrens + "]";
	}

	static public class Entry<V> {
		String prefix;
		V value;

		public Entry() {
		}

		public Entry(String p, V v) {
			prefix = p;
			value = v;
		}

		public String prefix() {
			return prefix;
		}

		public V value() {
			return value;
		}

		@Override
		public String toString() {
			return "Entry [prefix=" + prefix + ", value=" + value + "]";
		}

	}
}