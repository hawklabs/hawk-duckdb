package org.eclipse.hawk.duckdb.benchmarks.index;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;

import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;

public class KeysTableBenchmark extends AbstractIndexBenchmark {

	private static final String KEY_IDS_TABLE = "hawk_index_keys";

	public KeysTableBenchmark(DataGenerator gen, int rows, int queryIterations) {
		super(gen, rows, queryIterations);
	}

	@Override
	protected BenchmarkQuery findNodesByIndexKey() throws SQLException {
		return new PreparedByIndexBenchmarkQuery(duckDB,
			(idx) -> String.format(
				"SELECT * FROM idx_%s WHERE keyId = (SELECT id FROM %s WHERE key = ? LIMIT 1);",
				idx, KEY_IDS_TABLE));
	}

	@Override
	protected void loadData() throws IOException, SQLException {
		final Map<String, File> files = generator.csvsByIndexWithKeyIDs(nRows);
		copyKeyIDs(files);
		createIndexTables(files);
		copyDataIntoTables(files);
	}

	protected void copyKeyIDs(final Map<String, File> files) throws SQLException {
		File fKeyIDs = files.remove(DataGenerator.KEY_CSV_KEYIDS);
		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute(String.format(
				"COPY %s FROM '%s' (DELIMITER '|', HEADER);",
				KEY_IDS_TABLE, fKeyIDs.getAbsolutePath()));
		}
	}

	protected void copyDataIntoTables(final Map<String, File> writers) throws SQLException {
		try (Statement stmt = duckDB.createStatement()) {
			for (Entry<String, File> entry : writers.entrySet()) {
				stmt.execute(String.format(
					"COPY idx_%s FROM '%s' (DELIMITER '|', HEADER);",
					entry.getKey(),
					entry.getValue().getAbsolutePath()));
			}
		}
		duckDB.commit();
	}

	protected void createIndexTables(final Map<String, File> writers) throws SQLException {
		for (String idxName : writers.keySet()) {
			try (Statement stmt = duckDB.createStatement()) {
				stmt.execute(String.format(
					"CREATE TABLE idx_%s ("
					+ "keyid BIGINT NOT NULL,"
					+ "keyValue BIGINT NOT NULL,"
					+ "nodeId BIGINT NOT NULL"
					+ ");", idxName));

				final String sqlCreateIndex = String.format(
					"CREATE INDEX idx_%s_index ON idx_%s (keyid);",
					idxName, idxName
				);
				// CREATE INDEX idx_a_index ON idx_a (keyid);
				stmt.execute(sqlCreateIndex);
			}
		}
	}

	protected void createSchema() throws SQLException {
		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute(String.format(
				"CREATE TABLE %s ("
				+ "id BIGINT,"
				+ "key VARCHAR PRIMARY KEY"
				+ ");", KEY_IDS_TABLE));
		}
	}
}
