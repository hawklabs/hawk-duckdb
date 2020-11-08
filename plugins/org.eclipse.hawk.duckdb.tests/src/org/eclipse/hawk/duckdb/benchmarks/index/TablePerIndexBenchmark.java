package org.eclipse.hawk.duckdb.benchmarks.index;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;

import org.eclipse.hawk.duckdb.benchmarks.DataGenerator;

public class TablePerIndexBenchmark extends AbstractIndexBenchmark {

	public TablePerIndexBenchmark(DataGenerator gen, int rows, int queryIterations) {
		super(gen, rows, queryIterations);
	}

	@Override
	protected BenchmarkQuery findNodesByIndexKey() throws SQLException {
		return new PreparedByIndexBenchmarkQuery(duckDB,
			(idx) -> String.format("SELECT * FROM idx_%s WHERE keyName = ?;", idx));
	}

	@Override
	protected void loadData() throws IOException, SQLException {
		final Map<String, File> files = generator.csvsByIndex(nRows);
		createIndexTables(files);
		copyDataIntoTables(files);
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
					+ "keyName VARCHAR NOT NULL,"
					+ "keyValue BIGINT NOT NULL,"
					+ "nodeId BIGINT NOT NULL"
					+ ");", idxName));
			}
		}
	}

	protected void createSchema() throws SQLException {
		// nothing - the schema is created based on data
	}
}
