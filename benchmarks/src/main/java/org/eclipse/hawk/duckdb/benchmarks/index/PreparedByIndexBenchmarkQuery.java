package org.eclipse.hawk.duckdb.benchmarks.index;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.eclipse.hawk.duckdb.benchmarks.Row;

public class PreparedByIndexBenchmarkQuery implements BenchmarkQuery {
	private final Connection duckDB;
	private final Function<String, String> sqlGenerator;
	private Map<String, PreparedStatement> queries = new HashMap<>();

	public PreparedByIndexBenchmarkQuery(Connection duckDB, Function<String, String> sqlGeneratorFromIndex) {
		this.duckDB = duckDB;
		this.sqlGenerator = sqlGeneratorFromIndex;
	}

	@Override
	public void close() throws Exception {
		if (queries != null) {
			for (PreparedStatement stmt : queries.values()) {
				stmt.close();
			}
			queries = null;
		}
	}

	@Override
	public void run(Row row) throws SQLException {
		PreparedStatement query = queries.computeIfAbsent(row.indexName, (idx) -> {
			try {
				return duckDB.prepareStatement(sqlGenerator.apply(idx));
			} catch (SQLException e) {
				e.printStackTrace();
				return null;
			}
		});

		query.setString(1, row.keyName);
		query.execute();
	}
}