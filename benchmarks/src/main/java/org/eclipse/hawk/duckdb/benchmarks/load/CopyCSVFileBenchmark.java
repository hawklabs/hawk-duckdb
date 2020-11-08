package org.eclipse.hawk.duckdb.benchmarks.load;

import java.io.File;
import java.sql.Statement;

import org.eclipse.hawk.duckdb.benchmarks.Benchmark;
import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;

public class CopyCSVFileBenchmark extends AbstractLoadBenchmark implements Benchmark {

	public CopyCSVFileBenchmark(DataGenerator gen, int nRows) {
		super(gen, nRows, false);
	}

	@Override
	protected void loadData() throws Exception {
		File fCSV = File.createTempFile("duck", ".csv");
		fCSV.deleteOnExit();
		generator.csv(fCSV, nRows);

		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute(String.format("COPY data FROM '%s' (DELIMITER '|', HEADER);", fCSV.getAbsolutePath()));
		}
		duckDB.commit();
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}