package org.eclipse.hawk.duckdb.benchmarks.index;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.eclipse.hawk.duckdb.benchmarks.Benchmark;
import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;
import org.eclipse.hawk.duckdb.benchmarks.Row;

public abstract class AbstractIndexBenchmark implements Benchmark {

	protected final DataGenerator generator;

	protected abstract void createSchema() throws SQLException;
	protected abstract void loadData() throws IOException, SQLException;
	protected abstract BenchmarkQuery findNodesByIndexKey() throws SQLException;

	protected final int nRows;
	protected final int queryIterations;
	protected Connection duckDB;

	public AbstractIndexBenchmark(DataGenerator gen, int rows, int queryIterations) {
		this.generator = gen;
		this.nRows = rows;
		this.queryIterations = queryIterations;
	}

	@Override
	public long run() throws Exception {
		setupDatabase();

		final long loadStart = System.currentTimeMillis();
		loadData();
		final long loadEnd = System.currentTimeMillis();
		System.out.println(String.format("%s loaded %d rows in %d ms", this, nRows, loadEnd - loadStart));

		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute("BEGIN TRANSACTION");
		}

		final long millisStart = System.currentTimeMillis();
		runQuery(this::findNodesByIndexKey);
		final long millisEnd = System.currentTimeMillis();

		duckDB.close();

		return millisEnd - millisStart;
	}

	protected void setupDatabase() throws IOException, ClassNotFoundException, SQLException {
		File duckDBFile = File.createTempFile("duckbench", ".db");
		duckDBFile.delete();
		duckDBFile.deleteOnExit();
	
		Class.forName("org.duckdb.DuckDBDriver");
		this.duckDB = DriverManager.getConnection("jdbc:duckdb:" + duckDBFile.getPath());
		duckDB.setAutoCommit(false);

		createSchema();
	}

	protected void runQuery(final BenchmarkQuerySupplier qSupplier) throws SQLException, Exception {
		try (BenchmarkQuery query = qSupplier.get()) {
			for (int i = 0; i < queryIterations; i++) {
				Row row = generator.row();
				query.run(row);
			}
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

}