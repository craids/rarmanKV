
package rkv;

public class RKVDBException extends Exception {
	private static final long serialVersionUID = 1010101L;

	public RKVDBException(String message) {
		super(message);
	}

	public RKVDBException(String message, Throwable cause) {
		super(message, cause);
	}

	public RKVDBException(Throwable cause) {
		super(cause);
	}
}
