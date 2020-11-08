package org.eclipse.hawk.duckdb.benchmarks.load;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.eclipse.hawk.duckdb.benchmarks.Benchmark;
import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;

public abstract class AbstractLoadBenchmark implements Benchmark {

	protected final DataGenerator generator;
	protected final int nRows;
	protected final boolean autocommit;
	protected Connection duckDB;

	public AbstractLoadBenchmark(DataGenerator gen, int rows, boolean autocommit) {
		this.generator = gen;
		this.nRows = rows;
		this.autocommit = autocommit;
	}

	@Override
	public long run() throws Exception {
		setupDatabase();
		duckDB.setAutoCommit(autocommit);

		final long millisStart = System.currentTimeMillis();
		loadData();
		final long millisEnd = System.currentTimeMillis();
		checkRowCount();

		return millisEnd - millisStart;
	}

	protected void checkRowCount() throws SQLException {
		try (Statement stmt = duckDB.createStatement()) {
			ResultSet rs = stmt.executeQuery("SELECT COUNT(1) FROM data;");
			if (rs.next()) {
				final int countedRows = rs.getInt(1);
				if (countedRows != nRows) {
					throw new RuntimeException(String.format("Row count for %s is wrong: expected %d, got %d", this, nRows, countedRows));
				}
			} else {
				throw new RuntimeException("No results returned from row count check?");
			}
		}
	}

	protected void setupDatabase() throws IOException, ClassNotFoundException, SQLException {
		File duckDBFile = File.createTempFile("duckbench", ".db");
		duckDBFile.delete();
		duckDBFile.deleteOnExit();
	
		Class.forName("org.duckdb.DuckDBDriver");
		this.duckDB = DriverManager.getConnection("jdbc:duckdb:" + duckDBFile.getPath());
	
		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute("CREATE TABLE data ("
				+ "indexName VARCHAR NOT NULL,"
				+ "keyName VARCHAR NOT NULL,"
				+ "keyValue BIGINT NOT NULL,"
				+ "nodeId BIGINT NOT NULL"
				+ ");");
		}
	}

	protected abstract void loadData() throws Exception;

}