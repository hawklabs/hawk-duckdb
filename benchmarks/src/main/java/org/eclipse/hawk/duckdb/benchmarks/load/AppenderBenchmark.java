package org.eclipse.hawk.duckdb.benchmarks.load;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.eclipse.hawk.duckdb.benchmarks.Benchmark;
import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;
import org.eclipse.hawk.duckdb.benchmarks.Row;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class AppenderBenchmark extends AbstractLoadBenchmark implements Benchmark {
	public AppenderBenchmark(DataGenerator gen, int nRows) {
		super(gen, nRows, false);
	}

	@Override
	protected void loadData() throws SQLException {
		DuckDBConnection conn = (DuckDBConnection) duckDB;
		try (DuckDBAppender appender = conn.createAppender("main", "data")) {
			for (int i = 0; i < nRows; i++) {
				Row row = generator.row();

				appender.beginRow();
				appender.append(row.indexName);
				appender.append(row.keyName);
				appender.append(row.keyValue);
				appender.append(row.nodeId);
				appender.endRow();
			}
		}
	}

	@Override
	public String toString() {
		return String.format("AppenderBenchmark ", autocommit);
	}
}