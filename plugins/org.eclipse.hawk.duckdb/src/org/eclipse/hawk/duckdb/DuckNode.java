package org.eclipse.hawk.duckdb;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.eclipse.hawk.core.graph.IGraphDatabase;
import org.eclipse.hawk.core.graph.IGraphEdge;
import org.eclipse.hawk.core.graph.IGraphNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Represents a single graph node, as stored in DuckDB. Mutable state is not kept
 * in memory: instead, this are always queried on the fly.
 */
public class DuckNode extends AbstractDuckElement implements IGraphNode {

	private final class EdgeResultSetIterator implements Iterator<IGraphEdge> {
		private final ResultSet rs;
		Boolean hasNext;

		private EdgeResultSetIterator(ResultSet rs) {
			this.rs = rs;
		}

		@Override
		public boolean hasNext() {
			if (hasNext == null) {
				try {
					hasNext = rs.next();
				} catch (SQLException e) {
					LOGGER.error("Failed at fetching the next result", e);
					hasNext = false;
				}
			}
			return hasNext;
		}

		@Override
		public IGraphEdge next() {
			try {
				if (hasNext()) {
					final DuckEdge next = new DuckEdge(db, rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getLong(4));
					hasNext = null;
					return next;
				} else {
					throw new NoSuchElementException();
				}
			} catch (SQLException e) {
				LOGGER.error("Failed at fetching the next result", e);
				throw new NoSuchElementException();
			}
		}
	}

	private final class EdgeResultSetIterable implements Iterable<IGraphEdge> {
		private final String type;
		private final PreparedStatement stmt;

		private EdgeResultSetIterable(String type, PreparedStatement stmt) {
			this.type = type;
			this.stmt = stmt;
		}

		@Override
		public Iterator<IGraphEdge> iterator() {
			try {
				stmt.setLong(1, id);
				if (type != null) {
					stmt.setString(2, type);
				}
				final ResultSet rs = stmt.executeQuery();
				return new EdgeResultSetIterator(rs);
			} catch (SQLException e) {
				LOGGER.error("Failed to retrieve outgoing edges from node " + id, e);
			}

			return Collections.emptyIterator();
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(DuckNode.class);

	public DuckNode(DuckDatabase db, long nodeId) {
		super(db, nodeId);
	}

	@Override
	public Long getId() {
		return id;
	}

	@Override
	public Iterable<IGraphEdge> getEdges() {
		return getEdgesWithType(null);
	}

	@Override
	public Iterable<IGraphEdge> getEdgesWithType(String type) {
		Iterable<IGraphEdge> iterOutgoing = getOutgoingWithType(type);
		Iterable<IGraphEdge> iterIncoming = getIncomingWithType(type);
		return Iterables.concat(iterOutgoing, iterIncoming);
	}

	@Override
	public Iterable<IGraphEdge> getOutgoingWithType(String type) {
		final String sqlOutgoing = String.format(
			"SELECT id, label, from_node_id, to_node_id FROM %s WHERE from_node_id = ? %s;",
			DuckDatabase.TABLE_EDGES,
			type == null ? "" : "AND label = ?"
		);
		return getEdgeQueryIterable(type, sqlOutgoing);
	}

	@Override
	public Iterable<IGraphEdge> getIncomingWithType(String type) {
		final String sqlIncoming = String.format(
			"SELECT id, label, from_node_id, to_node_id FROM %s WHERE to_node_id = ? %s;",
			DuckDatabase.TABLE_EDGES,
			type == null ? "" : "AND label = ?"
		);

		return getEdgeQueryIterable(type, sqlIncoming);
	}

	private Iterable<IGraphEdge> getEdgeQueryIterable(String type, final String sqlOutgoing) {
		try {
			final PreparedStatement stmt = db.prepareSQL(sqlOutgoing);
			return new EdgeResultSetIterable(type, stmt);
		} catch (SQLException e) {
			LOGGER.error("Failed to parse query to retrieve outgoing edges from node " + id, e);
		}

		return null;
	}
	
	@Override
	public Iterable<IGraphEdge> getIncoming() {
		return getIncomingWithType(null);
	}

	@Override
	public Iterable<IGraphEdge> getOutgoing() {
		return getOutgoingWithType(null);
	}

	@Override
	public void delete() {
		try {
			deleteProperties();

			final String sqlDeleteNode = String.format(
				"DELETE FROM %s WHERE id = ?;",
				DuckDatabase.TABLE_NODES
			);
			try (PreparedStatement stmt = db.prepareSQL(sqlDeleteNode)) {
				stmt.setLong(1, id);
				stmt.execute();
			}

			final String sqlDeleteEdgeProperties = String.format(
				"DELETE FROM %s WHERE elem_id IN (SELECT id FROM %s WHERE from_node_id = ? OR to_node_id = ?);",
				DuckDatabase.TABLE_PROPERTIES, DuckDatabase.TABLE_EDGES
			);
			try (PreparedStatement stmt = db.prepareSQL(sqlDeleteEdgeProperties)) {
				stmt.setLong(1, id);
				stmt.setLong(2, id);
				stmt.execute();
			}

			final String sqlDeleteEdges = String.format(
				"DELETE FROM %s WHERE from_node_id = ? OR to_node_id = ?;",
				DuckDatabase.TABLE_EDGES
			);
			try (PreparedStatement stmt = db.prepareSQL(sqlDeleteEdges)) {
				stmt.setLong(1, id);
				stmt.setLong(2, id);
				stmt.execute();
			}
		} catch (SQLException e) {
			LOGGER.error("Failed to delete node " + id, e);
		}
	}

	@Override
	public IGraphDatabase getGraph() {
		return db;
	}

	@Override
	public String toString() {
		return "DuckNode [nodeId=" + id + "]";
	}
	
}
