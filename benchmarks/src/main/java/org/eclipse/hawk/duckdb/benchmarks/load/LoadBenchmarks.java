package org.eclipse.hawk.duckdb.benchmarks.load;

import java.util.Arrays;
import java.util.Random;

import org.eclipse.hawk.duckdb.benchmarks.Benchmark;
import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;

/**
 * Compares several ways to do large loads with DuckDB.
 */
public class LoadBenchmarks {

	public static void main(String[] args) throws Exception {
		final int nRows = 100_000;

		final Benchmark[] benchmarks = new Benchmark[] {
			// Far too slow!
			//new ManyPreparedStatementBenchmark(gen, nRows, true),

			// Easiest to do with Hawk's API, pretty slow though
			new ManyPreparedStatementBenchmark(createGenerator(), nRows, false),

			// Still pretty slow (autocommit is definitely bad)
			//new SinglePreparedStatementBenchmark(gen, nRows, true),

			// Fastest one that would be reasonably easy to integrate in the backend
			new SinglePreparedStatementBenchmark(createGenerator(), nRows, false),

			// Fastest one so far, by a lot (as mentioned by Gabor)
			new CopyCSVFileBenchmark(createGenerator(), nRows),

			// Fast and the most convenient but currently limited to number/string/bool types
			new AppenderBenchmark(createGenerator(), nRows),

			// This one breaks in DuckDB 0.2.2: Unknown Error: Maximum line size of 1048576 bytes exceeded!
			//new CopyCSVPipeBenchmark(createGenerator(), nRows),
		};

		for (Benchmark bench : benchmarks) {
			final long benchTime = bench.run();
			System.out.println(String.format("Benchmark %s: %d ms for %d rows", bench, benchTime, nRows));
		}
	}

	protected static DataGenerator createGenerator() {
		final Random rnd = new Random(42);
		final DataGenerator gen = DataGenerator.randomChoice(rnd,
			Arrays.asList("a", "b", "c"),
			Arrays.asList("k1", "k2", "k3"),
			0, 100,
			0, 10_000);
		return gen;
	}

}
