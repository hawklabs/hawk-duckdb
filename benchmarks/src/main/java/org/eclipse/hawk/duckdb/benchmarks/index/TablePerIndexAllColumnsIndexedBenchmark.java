package org.eclipse.hawk.duckdb.benchmarks.index;

import java.sql.SQLException;
import java.sql.Statement;
import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;

public class TablePerIndexAllColumnsIndexedBenchmark extends TablePerIndexBenchmark {

	public TablePerIndexAllColumnsIndexedBenchmark(DataGenerator gen, int rows, int queryIterations) {
		super(gen, rows, queryIterations);
	}

	@Override
	protected void createIndexTableIndices(String idxName, Statement stmt) throws SQLException {
		stmt.execute(String.format("CREATE INDEX idx_%s_keyidx ON idx_%s (keyName, keyValue, nodeId);", idxName, idxName));
	}
}
