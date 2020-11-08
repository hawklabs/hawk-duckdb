package org.eclipse.hawk.duckdb.benchmarks;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

public class DataGenerator {
	private Supplier<String> indexNameSupplier;
	private Supplier<String> keyNameSupplier;
	private Supplier<Long> keyValueSupplier;
	private Supplier<Long> nodeIdSupplier;

	private static final class StringIdentifierGenerator implements Function<String, Integer> {
		private final PrintWriter pwKeys;
		int keyIDCounter = 0;

		private StringIdentifierGenerator(PrintWriter pwKeys) {
			this.pwKeys = pwKeys;
		}

		@Override
		public Integer apply(String key) {
			int id = keyIDCounter++;
			pwKeys.println(id + "|" + key);					
			return id;
		}
	}

	private static class OutputFile {
		public final File file;
		public final PrintWriter writer;
		public OutputFile(File file, PrintWriter writer) {
			super();
			this.file = file;
			this.writer = writer;
		}
	}

	public DataGenerator(Supplier<String> idxNameSupplier, Supplier<String> keyNameSupplier,
			Supplier<Long> keyValueSupplier, Supplier<Long> nodeIdSupplier) {
		this.indexNameSupplier = idxNameSupplier;
		this.keyNameSupplier = keyNameSupplier;
		this.keyValueSupplier = keyValueSupplier;
		this.nodeIdSupplier = nodeIdSupplier;
	}

	public Row row() {
		return new Row(
			indexNameSupplier.get(),
			keyNameSupplier.get(),
			keyValueSupplier.get(),
			nodeIdSupplier.get()
		);
	}

	public void csv(File fCSV, int nRows) throws IOException {
		try (FileWriter fos = new FileWriter(fCSV, Charset.forName("UTF-8")); PrintWriter pw = new PrintWriter(fos)) {
			pw.println("indexName|keyName|keyValue|nodeId");
			for (int i = 0; i < nRows; i++) {
				Row row = row();
				pw.println(row.indexName + "|" + row.keyName + "|" + row.keyValue + "|" + row.nodeId);
			}
		}
	}

	public Map<String, File> csvsByIndex(int nRows) {
		final Map<String, OutputFile> writers = new HashMap<>();
		for (int i = 0; i < nRows; i++) {
			Row row = row();

			OutputFile outFile = writers.computeIfAbsent(row.indexName, (idxName) -> {
				try {
					File fCSV = File.createTempFile("duck", ".csv");
					fCSV.deleteOnExit();

					final PrintWriter pw = getWriter(fCSV);
					pw.println("keyName|keyValue|nodeId");
					return new OutputFile(fCSV, pw);
				} catch (IOException ex) {
					ex.printStackTrace();
					return null;
				}
			});

			outFile.writer.println(row.keyName + "|" + row.keyValue + "|" + row.nodeId);
		}

		Map<String, File> files = new HashMap<>();
		for (Entry<String, OutputFile> entry : writers.entrySet()) {
			files.put(entry.getKey(), entry.getValue().file);
			entry.getValue().writer.close();
		}

		return files;
	}

	public static final String KEY_CSV_KEYIDS = "_keyids";

	/**
	 * Returns a map from the name of the key to the relevant CSV file. There
	 * is an extra entry for the key IDs CSV file, which has the special key
	 * {@link #KEY_CSV_KEYIDS}. This should be removed and processed separately.
	 */
	public Map<String, File> csvsByIndexWithKeyIDs(int nRows) throws IOException {
		final Map<String, File> files = new HashMap<>();
		File fKeyIDs = File.createTempFile("duckkeys", ".csv");
		fKeyIDs.deleteOnExit();
		files.put(KEY_CSV_KEYIDS, fKeyIDs);
 
		try (PrintWriter pwKeys = getWriter(fKeyIDs)) {
			final Map<String, Integer> keyIDs = new HashMap<>();
			pwKeys.println("id|key");

			final Function<String, Integer> keyIDFunction = new StringIdentifierGenerator(pwKeys);
			
			final Map<String, OutputFile> writers = new HashMap<>();
			for (int i = 0; i < nRows; i++) {
				Row row = row();

				OutputFile outFile = writers.computeIfAbsent(row.indexName, (idxName) -> {
					try {
						File fCSV = File.createTempFile("duck", ".csv");
						fCSV.deleteOnExit();
						final PrintWriter pw = getWriter(fCSV);
						pw.println("keyId|keyValue|nodeId");
						return new OutputFile(fCSV, pw);
					} catch (IOException ex) {
						ex.printStackTrace();
						return null;
					}
				});

				final int keyID = keyIDs.computeIfAbsent(row.keyName, keyIDFunction);
				outFile.writer.println(keyID + "|" + row.keyValue + "|" + row.nodeId);
			}

			for (Entry<String, OutputFile> entry : writers.entrySet()) {
				files.put(entry.getKey(), entry.getValue().file);
				entry.getValue().writer.close();
			}
		}

		return files;
	}

	/**
	 * Creates a CSV for a table with indexID + keyID + keyVal + keyName, and
	 * writes the contents of the associated indexID + keyID files.
	 */
	public File csvStarSchema(int nRows, File fKeyIDs, File fIndexIDs) throws IOException {
		File fStar = File.createTempFile("duckstar", ".csv");
		fStar.deleteOnExit();

		try (PrintWriter pwStar = getWriter(fStar);
			 PrintWriter pwKeys = getWriter(fKeyIDs);
			 PrintWriter pwIndexes = getWriter(fIndexIDs)) {
			
			pwStar.println("indexId|keyId|keyValue|nodeId");
			pwKeys.println("id|key");
			pwIndexes.println("id|index");

			final Map<String, Integer> keyIDs = new HashMap<>();
			final Map<String, Integer> indexIDs = new HashMap<>();
			final Function<String, Integer> keyIDFunction = new StringIdentifierGenerator(pwKeys);
			final Function<String, Integer> indexIDFunction = new StringIdentifierGenerator(pwIndexes);

			for (int i = 0; i < nRows; i++) {
				final Row row = row();
				final int indexID = indexIDs.computeIfAbsent(row.indexName, indexIDFunction);
				final int keyID = keyIDs.computeIfAbsent(row.keyName, keyIDFunction);
				pwStar.println(indexID + "|" + keyID + "|" + row.keyValue + "|" + row.nodeId);
			}
		}

		return fStar;
	}

	protected PrintWriter getWriter(File fKeyIDs) throws IOException {
		return new PrintWriter(new FileWriter(fKeyIDs, Charset.forName("UTF-8")));
	}

	public static DataGenerator randomChoice(Random rnd, List<String> idxNames, List<String> keyNames, long minKeyVal, long maxKeyVal, long minNodeId, long maxNodeId) {
		return new DataGenerator(
			choiceRandom(rnd, idxNames),
			choiceRandom(rnd, keyNames),
			intervalRandom(rnd, minKeyVal, maxKeyVal),
			intervalRandom(rnd, minNodeId, maxNodeId)
		);
	}

	private static <T> Supplier<T> choiceRandom(Random rnd, List<T> l) {
		assert !l.isEmpty();
		return () -> l.get(rnd.nextInt(l.size()));
	}

	private static Supplier<Long> intervalRandom(Random rnd, long min, long max) {
		final long range = max - min + 1;
		return () -> min + (rnd.nextLong() % range);
	}
}