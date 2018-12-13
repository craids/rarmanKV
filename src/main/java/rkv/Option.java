package rkv;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Option<V> {
	static None none = new None();

	public static <V> Option<V> makeOption(V o) {
		if (o != null)
			return new Some<V>(o);
		else
			return (Option<V>) none;
	}

	public static <V> Option<V> makeOption() {
		return (Option<V>) none;
	}

	public boolean nonEmpty() {
		return false;
	}
}
