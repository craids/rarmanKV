
package rkv;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class FileUtils {

	static void createDirectoryIfNotExists(File directory) throws IOException {
		if (directory.exists()) {
			if (!directory.isDirectory()) {
				throw new IOException(directory.getName() + " is not a directory.");
			}
			return;
		}

		if (!directory.mkdirs()) {
			throw new IOException("Cannot create directory " + directory.getName());
		}
	}

	static List<Integer> listIndexFiles(File directory) {
		File[] files = directory.listFiles(file -> Constants.INDEX_FILE_PATTERN.matcher(file.getName()).matches());
		if (files == null)
			return Collections.emptyList();

		// sort in ascending order. we want the earliest index files to be processed
		// first.
		return Arrays.stream(files).sorted(Comparator.comparingInt(f -> getFileId(f, Constants.INDEX_FILE_PATTERN)))
				.map(f -> getFileId(f, Constants.INDEX_FILE_PATTERN)).collect(Collectors.toList());
	}

	static File[] listTombstoneFiles(File directory) {
		File[] files = directory.listFiles(file -> Constants.TOMBSTONE_FILE_PATTERN.matcher(file.getName()).matches());
		if (files == null)
			return new File[0];

		Comparator<File> comparator = Comparator.comparingInt(f -> getFileId(f, Constants.TOMBSTONE_FILE_PATTERN));
		Arrays.sort(files, comparator);
		return files;
	}

	static File[] listDataFiles(File directory) {
		return directory.listFiles(file -> Constants.DATA_FILE_PATTERN.matcher(file.getName()).matches());
	}

	private static int getFileId(File file, Pattern pattern) {
		Matcher matcher = pattern.matcher(file.getName());
		if (matcher.find()) {
			return Integer.valueOf(matcher.group(1));
		}

		throw new IllegalArgumentException("Cannot extract file id for file " + file.getPath());

	}
}
