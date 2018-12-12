
package rkv;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

class DBDirectory {

	private final File dbDirectory;
	private final FileChannel directoryChannel;

	private DBDirectory(File dbDirectory, FileChannel directoryChannel) {
		this.dbDirectory = dbDirectory;
		this.directoryChannel = directoryChannel;
	}

	static DBDirectory open(File directory) throws IOException {
		FileUtils.createDirectoryIfNotExists(directory);
		return new DBDirectory(directory, openReadOnlyChannel(directory));
	}

	void close() throws IOException {
		directoryChannel.close();
	}

	Path getPath() {
		return dbDirectory.toPath();
	}

	File[] listDataFiles() {
		return FileUtils.listDataFiles(dbDirectory);
	}

	List<Integer> listIndexFiles() {
		return FileUtils.listIndexFiles(dbDirectory);
	}

	File[] listTombstoneFiles() {
		return FileUtils.listTombstoneFiles(dbDirectory);
	}

	void syncMetaData() {
		try {
			directoryChannel.force(true);
		} catch (IOException e) {
		}
	}

	private static FileChannel openReadOnlyChannel(File dbDirectory) throws IOException {
		return FileChannel.open(dbDirectory.toPath(), StandardOpenOption.READ);
	}
}
