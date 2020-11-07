package org.eclipse.hawk.duckdb;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.eclipse.hawk.core.IConsole;
import org.eclipse.hawk.core.graph.IGraphDatabase;
import org.eclipse.hawk.core.graph.IGraphEdge;
import org.eclipse.hawk.core.graph.IGraphIterable;
import org.eclipse.hawk.core.graph.IGraphNode;
import org.eclipse.hawk.core.graph.IGraphNodeIndex;
import org.eclipse.hawk.core.graph.IGraphTransaction;
import org.eclipse.hawk.core.util.FileOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuckDatabase implements IGraphDatabase {

	private static final Logger LOGGER = LoggerFactory.getLogger(DuckDatabase.class);

	private static final String DUCKDB_FILENAME  = "duck.db";
	private static final String SEQUENCE_NODES   = "nodes_seq";
	private static final String TABLE_NODES      = "nodes";

	protected static final String TABLE_PROPERTIES = "properties";

	private File duckDBFile;
	private DuckTransaction tx;
	private Connection duckDB;

	@Override
	public String getHumanReadableName() {
		return "DuckDB Database";
	}

	@Override
	public String getPath() {
		return duckDBFile.getParent();
	}

	@Override
	public void run(File folder, IConsole c) {
		try {
			Class.forName("org.duckdb.DuckDBDriver");
			this.duckDBFile = new File(folder.getCanonicalFile(), DUCKDB_FILENAME);
			this.duckDB = DriverManager.getConnection("jdbc:duckdb:" + duckDBFile.getPath());

			// Disable autocommit - discouraged by DuckDB
			duckDB.setAutoCommit(false);

			// Create base schema
			try (Statement stmt = duckDB.createStatement()) {
				if (!tableExists(TABLE_NODES)) {
					initialiseSchema(stmt);
				}
			} catch (SQLException e) {
				LOGGER.error("Could not ensure the table exists", e);
			}
			
			tx = new DuckTransaction(duckDB);
			
		} catch (ClassNotFoundException e) {
			LOGGER.error("Could not find the class for the DuckDB driver", e);
		} catch (SQLException e) {
			LOGGER.error("Could not start the connection to DuckDB", e);
		} catch (IOException e) {
			LOGGER.error("Could not compute the path to the parent folder", e);
		}
	}

	private void initialiseSchema(Statement stmt) throws SQLException {
		stmt.execute(String.format(
				"CREATE SEQUENCE %s;",
				SEQUENCE_NODES));

		stmt.execute(String.format(
			"CREATE TABLE %s ("
			+ "  id BIGINT PRIMARY KEY,"
			+ "  label VARCHAR NOT NULL"
			+ ");",
			TABLE_NODES
			));
		
		stmt.execute(String.format(
			"CREATE INDEX %s_label ON %s (label);",
			TABLE_NODES, TABLE_NODES));

		stmt.execute(String.format(
			"CREATE TABLE %s ("
			+ "  node_id BIGINT NOT NULL,"
			+ "  name VARCHAR NOT NULL, %s,"
			+ "  PRIMARY KEY (node_id, name)"
			+ ");",
			TABLE_PROPERTIES, DuckNode.ValueType.sqlTableColumns()));

		stmt.execute(String.format(
			"CREATE INDEX %s_nodeid ON %s (node_id)",
			TABLE_PROPERTIES, TABLE_PROPERTIES));
	}

	@Override
	public void shutdown() throws Exception {
		duckDB.close();
	}

	@Override
	public void delete() throws Exception {
		shutdown();
		final boolean deleted = FileOperations.deleteFiles(duckDBFile.getParentFile(), true);
		LOGGER.info(deleted ? "Successfully deleted store {}" : "Failed to delete store {}", duckDBFile);
	}

	@Override
	public IGraphNodeIndex getOrCreateNodeIndex(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphNodeIndex getMetamodelIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphNodeIndex getFileIndex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphTransaction beginTransaction() throws Exception {
		tx.begin();
		return tx;
	}

	@Override
	public boolean isTransactional() {
		return true;
	}

	@Override
	public void enterBatchMode() {
		// no-op
	}

	@Override
	public void exitBatchMode() {
		// no-op
	}

	@Override
	public IGraphIterable<? extends IGraphNode> allNodes(String label) {

		return new IGraphIterable<DuckNode>() {

			@Override
			public Iterator<DuckNode> iterator() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public int size() {
				// TODO GABOR: why does this return *no rows* rather than a single row with 0 when no such nodes exist?
				try (PreparedStatement stmt = duckDB.prepareStatement("SELECT COUNT(1) FROM nodes WHERE label = ?;")) {
					stmt.setString(1, label);
					ResultSet rs = stmt.executeQuery();
					if (rs.next()) {
						return rs.getInt(1);
					}
				} catch (SQLException e) {
					LOGGER.error("Could not fetch the number of nodes with label " + label, e);
				}

				return 0;
			}

			@Override
			public DuckNode getSingle() {
				try (PreparedStatement stmt = duckDB.prepareStatement("SELECT id FROM nodes WHERE label = ? LIMIT 1;")) {
					stmt.setString(1, label);
					ResultSet rs = stmt.executeQuery();
					if (rs.next()) {
						return new DuckNode(duckDB, rs.getLong(1));
					}
				} catch (SQLException e) {
					LOGGER.error("Could not fetch the first node of label " + label, e);
				}

				return null;
			}
			
		};
	}

	@Override
	public IGraphNode createNode(Map<String, Object> props, String label) {
		try (PreparedStatement stmt = duckDB.prepareStatement(String.format("INSERT INTO %s (id, label) VALUES (?, ?);", TABLE_NODES))) {
			long nodeId = nextValue(SEQUENCE_NODES);
			stmt.setLong(1, nodeId);
			stmt.setString(2, label);
			stmt.execute();

			final DuckNode dn = new DuckNode(duckDB, nodeId);
			if (props != null) {
				for (Entry<String, Object> entry : props.entrySet()) {
					dn.setProperty(entry.getKey(), entry.getValue());
				}
			}

			return dn;
		} catch (SQLException e) {
			LOGGER.error("Failed to insert the node row", e);
			return null;
		}
	}


	@Override
	public IGraphEdge createRelationship(IGraphNode start, IGraphNode end, String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphEdge createRelationship(IGraphNode start, IGraphNode end, String type, Map<String, Object> props) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getGraph() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IGraphNode getNodeById(Object id) {
		if (id instanceof String) {
			return new DuckNode(duckDB, Long.valueOf((String) id));
		} else {
			return new DuckNode(duckDB, (long) id);
		}
	}

	@Override
	public boolean nodeIndexExists(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getTempDir() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Mode currentMode() {
		// TODO Auto-generated method stub
		return Mode.TX_MODE;
	}

	@Override
	public Set<String> getNodeIndexNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getKnownMMUris() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Returns the next value in a sequence.
	 */
	private long nextValue(String sequence) throws SQLException {
		try (PreparedStatement stmt = duckDB.prepareStatement("SELECT nextval(?);")) {
			stmt.setString(1, sequence);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			return rs.getLong(1);
		}
	}

	/**
	 * DuckDB does not support JDBC metadata yet, so we basically have
	 * to run a SELECT and use whether an exception happens or not.
	 */
	private boolean tableExists(String table) {
		try (Statement stmt = duckDB.createStatement()) {
			stmt.execute(String.format("SELECT 1 FROM %s LIMIT 1", table));
			return true;
		} catch (SQLException ex) {
			return false;
		}
	}
}
