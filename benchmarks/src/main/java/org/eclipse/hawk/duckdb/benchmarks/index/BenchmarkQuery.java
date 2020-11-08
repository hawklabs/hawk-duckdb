package org.eclipse.hawk.duckdb.benchmarks.index;

import java.sql.SQLException;

import org.eclipse.hawk.duckdb.benchmarks.Row;

public interface BenchmarkQuery extends AutoCloseable {

	void run(Row row) throws SQLException;

}
