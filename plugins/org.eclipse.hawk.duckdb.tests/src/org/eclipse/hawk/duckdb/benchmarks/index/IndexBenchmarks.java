package org.eclipse.hawk.duckdb.benchmarks.index;

import java.util.Arrays;
import java.util.Random;

import org.eclipse.hawk.duckdb.benchmarks.Benchmark;
import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;

public class IndexBenchmarks {

	public static void main(String[] args) throws Exception {
		final int nRows = 50_000;
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
			new SingleTableAllColumnsIndexedBenchmark(createGenerator(), nRows, nIterations),
			new SingleTableIndexKeyIndexedBenchmark(createGenerator(), nRows, nIterations),

			// This approach is faster as there are fewer rows to do a linear scan over
			new TablePerIndexBenchmark(createGenerator(), nRows, nIterations),
		};

		for (Benchmark bench : benchmarks) {
			final long benchTime = bench.run();
			System.out.println(String.format("Benchmark %s: %d ms for %d iterations over %d rows", bench, benchTime, nIterations, nRows));
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
