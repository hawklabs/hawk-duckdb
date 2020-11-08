package org.eclipse.hawk.duckdb.benchmarks.index;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class PreparedBenchmarkQuery implements BenchmarkQuery {
	protected PreparedStatement query;

	public PreparedBenchmarkQuery(Connection duckDB, String sql) throws SQLException {
		query = duckDB.prepareStatement(sql);
	}

	@Override
	public void close() throws Exception {
		if (query != null) {
			query.close();
			query = null;
		}
	}
}