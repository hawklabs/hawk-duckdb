package org.eclipse.hawk.duckdb.benchmarks.load;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Statement;

import org.eclipse.hawk.duckdb.benchmarks.Benchmark;
import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;
import org.eclipse.hawk.duckdb.benchmarks.Row;

public class CopyCSVFileBenchmark extends AbstractLoadBenchmark implements Benchmark {

	public CopyCSVFileBenchmark(DataGenerator gen, int nRows) {
		super(gen, nRows, false);
	}

	@Override
	protected void loadData() throws Exception {
		File fCSV = File.createTempFile("duck", ".csv");
		try (FileWriter fos = new FileWriter(fCSV, Charset.forName("UTF-8")); PrintWriter pw = new PrintWriter(fos)) {
			pw.println("indexName|keyName|keyValue|nodeId");
			for (int i = 0; i < nRows; i++) {
				Row row = generator.generateRow();
				pw.println(row.indexName + "|" + row.keyName + "|" + row.keyValue + "|" + row.nodeId);
			}
		}

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