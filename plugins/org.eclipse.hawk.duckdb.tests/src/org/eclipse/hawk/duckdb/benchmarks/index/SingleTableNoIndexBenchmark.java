package org.eclipse.hawk.duckdb.benchmarks.index;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;
import org.eclipse.hawk.duckdb.benchmarks.Row;

public class SingleTableNoIndexBenchmark extends AbstractIndexBenchmark {

	public SingleTableNoIndexBenchmark(DataGenerator gen, int rows, int queryIterations) {
		super(gen, rows, queryIterations);
	}

	@Override
	protected PreparedBenchmarkQuery findNodesByIndexKey() throws SQLException {
		return new PreparedBenchmarkQuery(duckDB, "SELECT * FROM data WHERE keyName = ? AND indexName = ?;") {
			@Override
			public void run(Row row) throws SQLException {
				query.setString(1, row.keyName);
				query.setString(2, row.indexName);
				query.executeQuery().close();
			}
		};
	}

	@Override
	protected void loadData() throws IOException, SQLException {
		File fCSV = File.createTempFile("duck", ".csv");
		fCSV.deleteOnExit();
		generator.csv(fCSV, nRows);

		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute(String.format("COPY data FROM '%s' (DELIMITER '|', HEADER);", fCSV.getAbsolutePath()));
		}
		duckDB.commit();
	}

	protected void createSchema() throws SQLException {
		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute("CREATE TABLE data ("
				+ "indexName VARCHAR NOT NULL,"
				+ "keyName VARCHAR NOT NULL,"
				+ "keyValue BIGINT NOT NULL,"
				+ "nodeId BIGINT NOT NULL"
				+ ");");
		}
	}
}
