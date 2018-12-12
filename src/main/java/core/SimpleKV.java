package core;

import rkv.*;

import java.util.Iterator;

public class SimpleKV implements KeyValue {
	
	private RKVDB db;
	
	public SimpleKV() {
		RKVDBOptions options = new RKVDBOptions();
        options.setMaxFileSize(1024 * 1024 * 1024);
        options.setFlushDataSizeBytes(10 * 1024 * 1024);
        options.setCompactionThresholdPerFile(0.7);
        options.setCompactionJobRate(50 * 1024 * 1024);
        options.setNumberOfRecords(100_000_000);
        options.setCleanUpTombstonesDuringOpen(true);
        options.setCleanUpInMemoryIndexOnClose(false);
        options.setUseMemoryPool(true);
        options.setMemoryPoolChunkSize(2 * 1024 * 1024);
        options.setFixedKeySize(64);

        String directory = "rkvdbf";
        try {
			db = RKVDB.open(directory, options);
		} catch (RKVDBException e) {
			e.printStackTrace();
		}
	}
	

	@Override
	public SimpleKV initAndMakeStore(String path) {
		return new SimpleKV();
	}

	@Override
	public void write(char[] key, char[] value) {
		try {
			db.put(new String(key).getBytes(), new String(value).getBytes());
		} catch (RKVDBException e) {
			e.printStackTrace();
		}
	}

	@Override
	public char[] read(char[] key) {
		try {
			return new String(db.get(new String(key).getBytes())).toCharArray();
		} catch (RKVDBException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Iterator<KVPair> readRange(char[] startKey, char[] endKey) {
		return null;
	}

	@Override
	public void beginTx() {
		// do nothing
	}

	@Override
	public void commit() {
	}
}
