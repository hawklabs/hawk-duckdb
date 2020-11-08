package org.eclipse.hawk.duckdb.benchmarks.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.eclipse.hawk.duckdb.benchmarks.Benchmark;
import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;

public class IndexBenchmarks {

	public static void main(String[] args) throws Exception {
		final int nRows = 200_000;
		final int nIterations = 5_000;

		final Benchmark[] benchmarks = new Benchmark[] {
			/*
			 * No real difference over the options that requested multi column indices
			 * for finding by indexName + key, since DuckDB right now only supports
			 * unidimensional indexes over integers. See here:
			 * 
			 * https://duckdb.org/docs/sql/indexes
			 */
			new SingleTableNoIndexBenchmark(createGenerator(), nRows, nIterations),
			//new SingleTableAllColumnsIndexedBenchmark(createGenerator(), nRows, nIterations),
			//new SingleTableIndexKeyIndexedBenchmark(createGenerator(), nRows, nIterations),

			// This approach is faster as there are fewer rows to do a linear scan over
			new TablePerIndexBenchmark(createGenerator(), nRows, nIterations),

			// No real difference for this one (strings aren't really indexed in DuckDB 0.2.2)
			new TablePerIndexWithKeyStringIndexBenchmark(createGenerator(), nRows, nIterations),

			// This seems to be slower than just using one table per benchmark when there are few distinct keys
			// (e.g. less than 20 or so in my Lenovo X1 laptop with an SSD), but much faster where there are
			// many distinct keys? EXPAIN suggests that linear scans are still being used, even when I manually
			// go into the DuckDB console and create an index.
			new KeysTableBenchmark(createGenerator(), nRows, nIterations),

			/*
			 * This seems to perform better than the single table approach, but it's still
			 * not as good as using one table per index.
			 */
			new StarSchemaBenchmark(createGenerator(), nRows, nIterations),
		};

		for (Benchmark bench : benchmarks) {
			final long benchTime = bench.run();
			System.out.println(String.format("Benchmark %s: ran %d iterations in %d ms over %d rows", bench, nIterations, benchTime, nRows));
		}
	}

	protected static DataGenerator createGenerator() {
		final Random rnd = new Random(42);

		final int nUniqueKeys = 50;
		final List<String> keys = new ArrayList<>();
		for (int i = 0; i < nUniqueKeys; i++) {
			keys.add("k" + i);
		}

		final DataGenerator gen = DataGenerator.randomChoice(rnd,
			Arrays.asList("a", "b", "c"),
			keys,
			0, 100,
			0, 10_000);
		return gen;
	}

}
