package core;

import rkv.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.SortedMap;
import java.net.*;
import java.nio.*;
import java.nio.channels.SocketChannel;

public class SimpleKV implements KeyValue {

	private final String dbFileName = "rkv.dat";
	private final String collectionName = "rkvstore";
	private final SortedMap<String, char[]> map;
	private final DB db;
	
	public SimpleKV() {
		db = DBMaker.openFile(dbFileName)
				.disableLocking()
				.disableTransactions()
				.enableHardCache()
				.closeOnExit()
				.make();
		map = db.createTreeMap(collectionName);
		
		try {
		InetSocketAddress crunchifyAddr = new InetSocketAddress("18.40.21.160", 4433);
		SocketChannel crunchifyClient = SocketChannel.open(crunchifyAddr);
 
		//log("Connecting to Server on port 1111...");
 
		ArrayList<String> companyDetails = new ArrayList<String>();
 
		// create a ArrayList with companyName list
		companyDetails.add("Facebook");
		companyDetails.add("Twitter");
		companyDetails.add("IBM");
		companyDetails.add("Google");
		companyDetails.add("CLOSE_CON");
 
		for (String companyName : companyDetails) {
 
			byte[] message = new String(companyName).getBytes();
			ByteBuffer buffer = ByteBuffer.wrap(message);
			crunchifyClient.write(buffer);
 
			//log("sending: " + companyName);
			buffer.clear();
 
			// wait for 2 seconds before sending next message
			Thread.sleep(2000);
		}
		crunchifyClient.close();
		}
		catch(Exception e)
		{
			
		}
	}
	

	@Override
	public SimpleKV initAndMakeStore(String path) {
		return new SimpleKV();
	}

	@Override
	public void write(char[] key, char[] value) {
		map.put(new String(key), value);
	}

	@Override
	public char[] read(char[] key) {
		return map.get(new String(key));
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
		db.commit();
	}
}
