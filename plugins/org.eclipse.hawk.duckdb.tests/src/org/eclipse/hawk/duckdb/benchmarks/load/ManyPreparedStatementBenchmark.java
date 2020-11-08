package org.eclipse.hawk.duckdb.benchmarks.load;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.eclipse.hawk.duckdb.benchmarks.Benchmark;
import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;
import org.eclipse.hawk.duckdb.benchmarks.Row;

public class ManyPreparedStatementBenchmark extends AbstractLoadBenchmark implements Benchmark {
	public ManyPreparedStatementBenchmark(DataGenerator gen, int nRows, boolean autocommit) {
		super(gen, nRows, autocommit);
	}

	@Override
	protected void loadData() throws SQLException {
		for (int i = 0; i < nRows; i++) {
			if (autocommit && i % 100 == 0) {
				System.out.println(String.format("%s: loaded %d out of %d rows", getClass().getSimpleName(), i, nRows));
			}
			Row row = generator.generateRow();
			try (PreparedStatement stmt = duckDB.prepareStatement("INSERT INTO data (indexName, keyName, keyValue, nodeId) VALUES (?, ?, ?, ?);")) {
				stmt.setString(1, row.indexName);
				stmt.setString(2, row.keyName);
				stmt.setLong(3, row.keyValue);
				stmt.setLong(4, row.nodeId);

				stmt.execute();
				assert stmt.getUpdateCount() == 1;
			}
		}
		if (!autocommit) {
			duckDB.commit();
		}
	}

	@Override
	public String toString() {
		return String.format("ManyPreparedStatementBenchmark [autocommit=%s]", autocommit);
	}
}