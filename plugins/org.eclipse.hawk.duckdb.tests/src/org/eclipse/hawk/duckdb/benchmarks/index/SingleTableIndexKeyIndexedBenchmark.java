package org.eclipse.hawk.duckdb.benchmarks.index;

import java.sql.SQLException;
import java.sql.Statement;

import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;

public class SingleTableIndexKeyIndexedBenchmark extends SingleTableNoIndexBenchmark {

	public SingleTableIndexKeyIndexedBenchmark(DataGenerator gen, int rows, int queryIterations) {
		super(gen, rows, queryIterations);
	}

	protected void createSchema() throws SQLException {
		super.createSchema();
		
		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute("CREATE INDEX data_index ON data (indexName, keyName);");
		}
	}
}
