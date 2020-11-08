package org.eclipse.hawk.duckdb.benchmarks;

import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

public class DataGenerator {
	private Supplier<String> indexNameSupplier;
	private Supplier<String> keyNameSupplier;
	private Supplier<Long> keyValueSupplier;
	private Supplier<Long> nodeIdSupplier;

	public DataGenerator(Supplier<String> idxNameSupplier, Supplier<String> keyNameSupplier,
			Supplier<Long> keyValueSupplier, Supplier<Long> nodeIdSupplier) {
		this.indexNameSupplier = idxNameSupplier;
		this.keyNameSupplier = keyNameSupplier;
		this.keyValueSupplier = keyValueSupplier;
		this.nodeIdSupplier = nodeIdSupplier;
	}

	public Row generateRow() {
		return new Row(
			indexNameSupplier.get(),
			keyNameSupplier.get(),
			keyValueSupplier.get(),
			nodeIdSupplier.get()
		);
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