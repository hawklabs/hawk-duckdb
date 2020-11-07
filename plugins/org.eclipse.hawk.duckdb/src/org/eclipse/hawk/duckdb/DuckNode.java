package org.eclipse.hawk.duckdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.eclipse.hawk.core.graph.IGraphDatabase;
import org.eclipse.hawk.core.graph.IGraphEdge;
import org.eclipse.hawk.core.graph.IGraphNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a single graph node, as stored in DuckDB. Mutable state is not kept
 * in memory: instead, this are always queried on the fly.
 */
public class DuckNode implements IGraphNode {

	private static final Logger LOGGER = LoggerFactory.getLogger(DuckNode.class);

	private final long nodeId;
	private final Connection duckDB;

	public DuckNode(Connection duckDB, long nodeId) {
		this.duckDB = duckDB;
		this.nodeId = nodeId;
	}

	@Override
	public Long getId() {
		return nodeId;
	}

	@Override
	public Set<String> getPropertyKeys() {
		String sqlQuery = String.format(
			"SELECT name FROM %s WHERE node_id = ?;",
			DuckDatabase.TABLE_PROPERTIES
		);
		
		try (PreparedStatement query = duckDB.prepareStatement(sqlQuery)) {
			query.setLong(1, nodeId);

			ResultSet rs = query.executeQuery();
			Set<String> keys = new HashSet<>();
			while (rs.next()) {
				keys.add(rs.getString(1));
			}
			return keys;
		} catch (SQLException e) {
			LOGGER.error("Could not list the property keys for node " + nodeId, e);
			return Collections.emptySet();
		}
	}

	@Override
	public Object getProperty(String name) {
		final String valueQuery = String.format(
			"SELECT %s FROM %s WHERE node_id = ? AND name = ?;",
			PropertyValueType.sqlQueryColumns(),
			DuckDatabase.TABLE_PROPERTIES);

		try (PreparedStatement vq = duckDB.prepareStatement(valueQuery)) {
			vq.setLong(1, nodeId);
			vq.setString(2, name);

			ResultSet rs = vq.executeQuery();
			if (rs.next()) {
				int i = 0;
				for (PropertyValueType vt : PropertyValueType.values()) {
					// JDBC column indices are 1-based
					i++;

					Object o = vt.getValue(rs, i);
					if (o != null) return o;
				}
			}
		} catch (SQLException | ClassNotFoundException | IOException e) {
			LOGGER.error("Could not fetch property " + name + " on node " + nodeId, e);
		}
		
		return null;
	}

	@Override
	public void setProperty(String name, Object value) {
		if (value == null) {
			removeProperty(name);
			return;
		}

		final PropertyValueType vt = PropertyValueType.from(value);
		final String updateQuery = String.format(
			"UPDATE %s SET %s = ?%s WHERE node_id = ? AND name = ?;",
			DuckDatabase.TABLE_PROPERTIES, vt.getColumnName(), vt == PropertyValueType.BLOB ? "::BLOB" : "");

		try (PreparedStatement update = duckDB.prepareStatement(updateQuery)) {
			// TODO GABOR: I have to put the new value as the *third* element in
			// the UPDATE, or I get conversion errors mentioning the wrong value
			// - this looks like a bug in the JDBC driver?
			vt.setParameter(update, 3, value);
			update.setLong(1, nodeId);
			update.setString(2, name);

			final int rowsChanged = update.executeUpdate();
			if (rowsChanged == 0) {
				// No rows were updated: do an insert
				final String insertQuery = String.format(
					"INSERT INTO %s (node_id, name, %s) VALUES (?, ?, ?);",
					DuckDatabase.TABLE_PROPERTIES, vt.getColumnName());

				try (PreparedStatement insert = duckDB.prepareStatement(insertQuery)) {
					insert.setLong(1, nodeId);
					insert.setString(2, name);
					vt.setParameter(insert, 3, value);
					insert.executeUpdate();
				}
			}
		} catch (SQLException | IOException e) {
			LOGGER.error("Could not update property " + name + " on node " + nodeId, e);
		}
	}

	@Override
	public Iterable<IGraphEdge> getEdges() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<IGraphEdge> getEdgesWithType(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<IGraphEdge> getOutgoingWithType(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<IGraphEdge> getIncomingWithType(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<IGraphEdge> getIncoming() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<IGraphEdge> getOutgoing() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete() {
		try {
			String sqlDeleteProps = String.format(
				"DELETE FROM %s WHERE node_id = ?;",
				DuckDatabase.TABLE_PROPERTIES
			);
			try (PreparedStatement stmt = duckDB.prepareStatement(sqlDeleteProps)) {
				stmt.setLong(1, nodeId);
				stmt.execute();
			}

			String sqlDeleteNode = String.format(
				"DELETE FROM %s WHERE id = ?;",
				DuckDatabase.TABLE_NODES
			);
			try (PreparedStatement stmt = duckDB.prepareStatement(sqlDeleteNode)) {
				stmt.setLong(1, nodeId);
				stmt.execute();
			}

			// TODO: delete edge properties and edges as well
		} catch (SQLException e) {
			LOGGER.error("Failed to delete node " + nodeId, e);
		}
	}

	@Override
	public IGraphDatabase getGraph() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeProperty(String name) {
		final String sqlDelete = String.format(
			"DELETE FROM %s WHERE node_id = ? AND name = ?;",
			DuckDatabase.TABLE_PROPERTIES
		);

		try (PreparedStatement stmt = duckDB.prepareStatement(sqlDelete)) {
			stmt.setLong(1, nodeId);
			stmt.setString(2, name);
			stmt.execute();
		} catch (SQLException e) {
			LOGGER.error("Could not remove property " + name + " from node " + nodeId, e);
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(nodeId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DuckNode other = (DuckNode) obj;
		return nodeId == other.nodeId;
	}

	@Override
	public String toString() {
		return "DuckNode [nodeId=" + nodeId + "]";
	}
	
}
