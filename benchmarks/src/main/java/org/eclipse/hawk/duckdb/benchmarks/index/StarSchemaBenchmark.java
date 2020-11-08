package org.eclipse.hawk.duckdb.benchmarks.index;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;

public class StarSchemaBenchmark extends AbstractIndexBenchmark {

	private static final String KEY_IDS_TABLE = "hawk_keys";
	private static final String INDEX_IDS_TABLE = "hawk_indices";
	private static final String DATA_TABLE = "data";

	public StarSchemaBenchmark(DataGenerator gen, int rows, int queryIterations) {
		super(gen, rows, queryIterations);
	}

	@Override
	protected BenchmarkQuery findNodesByIndexKey() throws SQLException {
		return new PreparedByIndexBenchmarkQuery(duckDB,
			(idx) -> String.format(
				"SELECT * FROM data WHERE keyId = (SELECT id FROM %s WHERE key = ? LIMIT 1) AND indexId = (SELECT id FROM %s WHERE index = '%s');",
				KEY_IDS_TABLE, INDEX_IDS_TABLE, idx));
	}

	@Override
	protected void loadData() throws IOException, SQLException {
		File fKeyIDs = File.createTempFile("duckkeys", ".csv");
		fKeyIDs.deleteOnExit();
		File fIndexIDs = File.createTempFile("duckidx", ".csv");
		fIndexIDs.deleteOnExit();
		File fStar = generator.csvStarSchema(nRows, fKeyIDs, fIndexIDs);

		copyCSV(KEY_IDS_TABLE, fKeyIDs);
		copyCSV(INDEX_IDS_TABLE, fIndexIDs);
		copyCSV(DATA_TABLE, fStar);

		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute(String.format(
				"CREATE INDEX i_data ON %s (indexId, keyId);",
				DATA_TABLE
			));
		}

		duckDB.commit();
	}

	protected void copyCSV(String keyIdsTable, File fKeyIDs) throws SQLException {
		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute(String.format(
				"COPY %s FROM '%s' (DELIMITER '|', HEADER);",
				keyIdsTable, fKeyIDs.getAbsolutePath()));
		}
	}

	protected void createSchema() throws SQLException {
		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute(String.format(
				"CREATE TABLE %s ("
				+ "id BIGINT UNIQUE,"
				+ "key VARCHAR PRIMARY KEY"
				+ ");", KEY_IDS_TABLE));

			stmt.execute(String.format(
					"CREATE TABLE %s ("
					+ "id BIGINT UNIQUE,"
					+ "index VARCHAR PRIMARY KEY"
					+ ");", INDEX_IDS_TABLE));

			stmt.execute(String.format(
					"CREATE TABLE %s ("
					+ "indexId BIGINT NOT NULL,"
					+ "keyId BIGINT NOT NULL,"
					+ "keyValue BIGINT NOT NULL,"
					+ "nodeId BIGINT NOT NULL"
						+ ");", DATA_TABLE));
		}
	}
}
