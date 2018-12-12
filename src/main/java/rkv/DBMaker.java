
package rkv;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOError;
import java.security.spec.KeySpec;

public class DBMaker {

	private byte cacheType = DBCacheRef.MRU;
	private int mruCacheSize = 2048;

	private String location = null;

	private boolean disableTransactions = false;
	private boolean lockingDisabled = false;
	private boolean readonly = false;
	private String password = null;
	private boolean useAES256Bit = true;
	private boolean useRandomAccessFile = false;
	private boolean autoClearRefCacheOnLowMem = true;
	private boolean closeOnJVMExit = false;
	private boolean deleteFilesAfterCloseFlag = false;

	private DBMaker() {
	}

	public static DBMaker openFile(String file) {
		DBMaker m = new DBMaker();
		m.location = file;
		return m;
	}

	public static DBMaker openMemory() {
		return new DBMaker();
	}

	public static DBMaker openZip(String zip) {
		DBMaker m = new DBMaker();
		m.location = "$$ZIP$$://" + zip;
		return m;
	}

	static String isZipFileLocation(String location) {
		String match = "$$ZIP$$://";
		if (location.startsWith(match)) {
			return location.substring(match.length());
		}
		return null;
	}

	public DBMaker enableWeakCache() {
		cacheType = DBCacheRef.WEAK;
		return this;
	}

	public DBMaker enableSoftCache() {
		cacheType = DBCacheRef.SOFT;
		return this;
	}

	public DBMaker enableHardCache() {
		cacheType = DBCacheRef.HARD;
		return this;
	}

	public DBMaker enableMRUCache() {
		cacheType = DBCacheRef.MRU;
		return this;
	}

	public DBMaker setMRUCacheSize(int cacheSize) {
		if (cacheSize < 0)
			throw new IllegalArgumentException("Cache size is smaller than zero");
		cacheType = DBCacheRef.MRU;
		mruCacheSize = cacheSize;
		return this;
	}

	public DBMaker disableCacheAutoClear() {
		this.autoClearRefCacheOnLowMem = false;
		return this;
	}

	public DBMaker enableEncryption(String password, boolean useAES256Bit) {
		this.password = password;
		this.useAES256Bit = useAES256Bit;
		return this;
	}

	public DBMaker readonly() {
		readonly = true;
		return this;
	}

	public DBMaker disableCache() {
		cacheType = DBCacheRef.NONE;
		return this;
	}

	public DBMaker disableTransactions() {
		this.disableTransactions = true;
		return this;
	}

	public DBMaker disableLocking() {
		this.lockingDisabled = true;
		return this;
	}

	public DBMaker useRandomAccessFile() {
		this.useRandomAccessFile = true;
		return this;
	}

	public DBMaker closeOnExit() {
		this.closeOnJVMExit = true;
		return this;
	}

	public DBMaker deleteFilesAfterClose() {
		this.deleteFilesAfterCloseFlag = true;
		return this;
	}

	public DB make() {

		Cipher cipherIn = null;
		Cipher cipherOut = null;
		if (password != null)
			try {
				// initialize ciphers
				// this code comes from stack owerflow
				// http://stackoverflow.com/questions/992019/java-256bit-aes-encryption/992413#992413
				byte[] salt = new byte[] { 3, -34, 123, 53, 78, 121, -12, -1, 45, -12, -48, 89, 11, 100, 99, 8 };

				SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
				KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, 1024, useAES256Bit ? 256 : 128);
				SecretKey tmp = factory.generateSecret(spec);
				SecretKey secret = new SecretKeySpec(tmp.getEncoded(), "AES");

				String transform = "AES/CBC/NoPadding";
				IvParameterSpec params = new IvParameterSpec(salt);

				cipherIn = Cipher.getInstance(transform);
				cipherIn.init(Cipher.ENCRYPT_MODE, secret, params);

				cipherOut = Cipher.getInstance(transform);
				cipherOut.init(Cipher.DECRYPT_MODE, secret, params);

				// sanity check, try with page size
				byte[] data = new byte[Storage.PAGE_SIZE];
				byte[] encData = cipherIn.doFinal(data);
				if (encData.length != Storage.PAGE_SIZE)
					throw new Error("Page size changed after encryption, make sure you use '/NoPadding'");
				byte[] data2 = cipherOut.doFinal(encData);
				for (int i = 0; i < data.length; i++) {
					if (data[i] != data2[i])
						throw new Error("Encryption provided by JRE does not work");
				}

			} catch (Exception e) {
				throw new IOError(e);
			}

		DBAbstract db = null;

		if (cacheType == DBCacheRef.MRU) {
			db = new DBCacheMRU(location, readonly, disableTransactions, cipherIn, cipherOut, useRandomAccessFile,
					deleteFilesAfterCloseFlag, mruCacheSize, lockingDisabled);
		} else if (cacheType == DBCacheRef.SOFT || cacheType == DBCacheRef.HARD || cacheType == DBCacheRef.WEAK) {
			db = new DBCacheRef(location, readonly, disableTransactions, cipherIn, cipherOut, useRandomAccessFile,
					deleteFilesAfterCloseFlag, cacheType, autoClearRefCacheOnLowMem, lockingDisabled);
		} else if (cacheType == DBCacheRef.NONE) {
			db = new DBStore(location, readonly, disableTransactions, cipherIn, cipherOut, useRandomAccessFile,
					deleteFilesAfterCloseFlag, lockingDisabled);
		} else {
			throw new IllegalArgumentException("Unknown cache type: " + cacheType);
		}

		if (closeOnJVMExit) {
			db.addShutdownHook();
		}

		return db;
	}

}
