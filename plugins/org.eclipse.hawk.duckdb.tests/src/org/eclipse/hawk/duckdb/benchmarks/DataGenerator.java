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
import java.util.function.Supplier;

public class DataGenerator {
	private Supplier<String> indexNameSupplier;
	private Supplier<String> keyNameSupplier;
	private Supplier<Long> keyValueSupplier;
	private Supplier<Long> nodeIdSupplier;

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
					return new OutputFile(fCSV, new PrintWriter(new FileWriter(fCSV, Charset.forName("UTF-8"))));
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
		return () -> min + rnd.nextLong() % range;
	}
}