
package rkv;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public final class RKVDB {

	private RKVDBInternal dbInternal;

	private File directory;

	public static RKVDB open(File dirname, RKVDBOptions opts) throws RKVDBException {
		RKVDB db = new RKVDB();
		try {
			db.dbInternal = RKVDBInternal.open(dirname, opts);
			db.directory = dirname;
		} catch (IOException e) {
			throw new RKVDBException("Failed to open db " + dirname.getName(), e);
		}
		return db;
	}

	public static RKVDB open(String directory, RKVDBOptions opts) throws RKVDBException {
		return RKVDB.open(new File(directory), opts);
	}

	public byte[] get(byte[] key) throws RKVDBException {
		try {
			return dbInternal.get(key, 1);
		} catch (IOException e) {
			throw new RKVDBException("Lookup failed.", e);
		}
	}

	public boolean put(byte[] key, byte[] value) throws RKVDBException {
		try {
			return dbInternal.put(key, value);
		} catch (IOException e) {
			throw new RKVDBException("Store to db failed.", e);
		}
	}

	public void delete(byte[] key) throws RKVDBException {
		try {
			dbInternal.delete(key);
		} catch (IOException e) {
			throw new RKVDBException("Delete operation failed.", e);
		}
	}

	public void close() throws RKVDBException {
		try {
			dbInternal.close();
		} catch (IOException e) {
			throw new RKVDBException("Error while closing " + directory.getName(), e);
		}
	}

	public long size() {
		return dbInternal.size();
	}

	public RKVDBStats stats() {
		return dbInternal.stats();
	}

	public void resetStats() {
		dbInternal.resetStats();
	}

	public RKVDBIterator newIterator() throws RKVDBException {
		return new RKVDBIterator(dbInternal);
	}

	// methods used in tests.

	@VisibleForTesting
	boolean isCompactionComplete() {
		return dbInternal.isCompactionComplete();
	}
}
