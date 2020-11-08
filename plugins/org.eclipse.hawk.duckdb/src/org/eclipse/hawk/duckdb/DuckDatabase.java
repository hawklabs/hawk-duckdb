package org.eclipse.hawk.duckdb;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

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

	private static final String DUCKDB_FILENAME   = "duck.db";
	private static final String SEQUENCE_ELEMENTS = "elems_seq";

	protected static final String TABLE_NODES = "nodes";
	protected static final String TABLE_EDGES = "edges";
	protected static final String TABLE_PROPERTIES = "properties";

	// turn to true to see all SQL printed on the console
	static final boolean DEBUG_SQL = true;

	private Connection duckDB;

	private File duckDBFile;
	private DuckTransaction tx;
	private Mode mode = Mode.NO_TX_MODE;

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
			
			// By default, we're on transactional mode
			tx = new DuckTransaction(duckDB);
			exitBatchMode();
		} catch (ClassNotFoundException e) {
			LOGGER.error("Could not find the class for the DuckDB driver", e);
		} catch (SQLException e) {
			LOGGER.error("Could not start the connection to DuckDB", e);
		} catch (IOException e) {
			LOGGER.error("Could not compute the path to the parent folder", e);
		}
	}

	private void initialiseSchema(Statement stmt) throws SQLException {
		createSequence(stmt, SEQUENCE_ELEMENTS);

		// Nodes
		runSQL(stmt, String.format(
			"CREATE TABLE %s ("
			+ "  id BIGINT PRIMARY KEY,"
			+ "  label VARCHAR NOT NULL"
			+ ");",
			TABLE_NODES
			));

		createIndex(stmt, TABLE_NODES, "label", "label");

		// Edges

		runSQL(stmt, String.format(
			"CREATE TABLE %s ("
			+ "id BIGINT PRIMARY KEY,"
			+ "from_node_id BIGINT NOT NULL,"
			+ "to_node_id BIGINT NOT NULL,"
			+ "label STRING NOT NULL,"
			+ "UNIQUE (from_node_id, to_node_id, label)"
			+ ");",
			TABLE_EDGES));

		createIndex(stmt, TABLE_EDGES, "outgoing", "from_node_id", "label");
		createIndex(stmt, TABLE_EDGES, "incoming", "to_node_id", "label");

		// Properties

		runSQL(stmt, String.format(
			"CREATE TABLE %s ("
			+ "  elem_id BIGINT NOT NULL,"
			+ "  name VARCHAR NOT NULL, %s,"
			+ "  PRIMARY KEY (elem_id, name)"
			+ ");",
			TABLE_PROPERTIES, PropertyValueType.sqlTableColumns()));

		createIndex(stmt, TABLE_PROPERTIES, "elemid", "elem_id");
	}

	private void createIndex(Statement stmt, String table, String idxSuffix, String... keys) throws SQLException {
		runSQL(stmt, String.format(
			"CREATE INDEX %s_%s ON %s (%s);",
			table, idxSuffix, table, String.join(", ", Arrays.asList(keys))
		));
	}

	private void createSequence(Statement stmt, final String seq) throws SQLException {
		runSQL(stmt, String.format("CREATE SEQUENCE %s;", seq));
	}

	protected void runSQL(Statement stmt, final String sql) throws SQLException {
		stmt.execute(sql);
		if (DEBUG_SQL) {
			System.out.println(sql);
		}
	}

	protected PreparedStatement prepareSQL(final String sql) throws SQLException {
		if (DEBUG_SQL) {
			System.out.println(sql);
		}
		return duckDB.prepareStatement(sql);
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
		return getOrCreateNodeIndex("hawk_metamodels");
	}

	@Override
	public IGraphNodeIndex getFileIndex() {
		return getOrCreateNodeIndex("hawk_files");
	}

	@Override
	public IGraphTransaction beginTransaction() throws Exception {
		if (mode == Mode.NO_TX_MODE) {
			exitBatchMode();
		}
		tx.begin();
		return tx;
	}

	@Override
	public boolean isTransactional() {
		return true;
	}

	@Override
	public void enterBatchMode() {
		// no-op for now (may switch to using CSVs in the future)
		mode = Mode.NO_TX_MODE;
	}

	@Override
	public void exitBatchMode() {
		// no-op for now
		mode = Mode.TX_MODE;
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
				try (PreparedStatement stmt = prepareSQL("SELECT COUNT(1) FROM nodes WHERE label = ?;")) {
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
				try (PreparedStatement stmt = prepareSQL("SELECT id FROM nodes WHERE label = ? LIMIT 1;")) {
					stmt.setString(1, label);
					ResultSet rs = stmt.executeQuery();
					if (rs.next()) {
						return new DuckNode(DuckDatabase.this, rs.getLong(1));
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
		try (PreparedStatement stmt = prepareSQL(String.format("INSERT INTO %s (id, label) VALUES (?, ?);", TABLE_NODES))) {
			long nodeId = nextValue(SEQUENCE_ELEMENTS);
			stmt.setLong(1, nodeId);
			stmt.setString(2, label);
			stmt.execute();

			final DuckNode dn = new DuckNode(this, nodeId);
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
		return createRelationship(start, end, type, null);
	}

	@Override
	public IGraphEdge createRelationship(IGraphNode start, IGraphNode end, String type, Map<String, Object> props) {
		final DuckNode dStart = (DuckNode) start;
		final DuckNode dEnd = (DuckNode) end;
		final long startId = dStart.getId();
		final long endId = dEnd.getId();

		try {
			Function<Long, DuckEdge> createEdge = (id) -> {
				DuckEdge edge = new DuckEdge(this, id, type, startId, endId);
				
				if (props != null) {
					for (Entry<String, Object> entry : props.entrySet()) {
						edge.setProperty(entry.getKey(), entry.getValue());
					}
				}

				return edge;
			};

			final String sqlFindExisting = String.format(
				"SELECT id FROM %s WHERE from_node_id = ? AND to_node_id = ? AND label = ?;",
				TABLE_EDGES);

			try (PreparedStatement stmt = prepareSQL(sqlFindExisting)) {
				stmt.setLong(1, startId);
				stmt.setLong(2, endId);
				stmt.setString(3, type);

				ResultSet rs = stmt.executeQuery();
				if  (rs.next()) {
					return createEdge.apply(rs.getLong(1));
				}
			}

			final String sqlAddNewEdge = String.format(
				"INSERT INTO %s (id, from_node_id, to_node_id, label) VALUES (?, ?, ?, ?);",
				TABLE_EDGES);

			try (PreparedStatement stmt = prepareSQL(sqlAddNewEdge)) {
				final long newEdgeId = nextValue(SEQUENCE_ELEMENTS);

				stmt.setLong(1, newEdgeId);
				stmt.setLong(2, startId);
				stmt.setLong(3, endId);
				stmt.setString(4, type);
				stmt.execute();

				return createEdge.apply(newEdgeId);
			}

		} catch (SQLException e) {
			LOGGER.error(String.format(
				"Failed to add a new edge from %d to %d of type %s",
				startId, endId, type), e);
		}		
		
		return null;
	}

	@Override
	public Object getGraph() {
		return duckDB;
	}

	@Override
	public IGraphNode getNodeById(Object id) {
		if (id instanceof String) {
			return new DuckNode(this, Long.valueOf((String) id));
		} else {
			return new DuckNode(this, (long) id);
		}
	}

	@Override
	public boolean nodeIndexExists(String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getTempDir() {
		return new File(duckDBFile.getParentFile(), "temp").getAbsolutePath();
	}

	@Override
	public Mode currentMode() {
		return mode;
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
		try (PreparedStatement stmt = prepareSQL("SELECT nextval(?);")) {
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
			runSQL(stmt, String.format("SELECT 1 FROM %s LIMIT 1", table));
			return true;
		} catch (SQLException ex) {
			return false;
		}
	}
}
