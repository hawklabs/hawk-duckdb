package org.eclipse.hawk.duckdb.benchmarks.index;

import java.sql.SQLException;

public interface BenchmarkQuerySupplier {
	BenchmarkQuery get() throws SQLException;
}