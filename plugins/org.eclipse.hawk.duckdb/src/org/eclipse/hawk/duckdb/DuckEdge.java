package org.eclipse.hawk.duckdb;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.eclipse.hawk.core.graph.IGraphEdge;
import org.eclipse.hawk.core.graph.IGraphNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a single edge, as stored in DuckDB. Mutable state is not kept in the object:
 * instead, it is always queried on the fly from DuckDB.
 */
public class DuckEdge extends AbstractDuckElement implements IGraphEdge {

	private static final Logger LOGGER = LoggerFactory.getLogger(DuckEdge.class);

	private final String type;
	private final long startNodeId;
	private final long endNodeId;

	public DuckEdge(DuckDatabase db, long id, String type, long startNodeId, long endNodeId) {
		super(db, id);
		this.type = type;
		this.startNodeId = startNodeId;
		this.endNodeId = endNodeId;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public IGraphNode getStartNode() {
		return new DuckNode(db, startNodeId);
	}

	@Override
	public IGraphNode getEndNode() {
		return new DuckNode(db, endNodeId);
	}

	@Override
	public void delete() {
		try {
			deleteProperties();

			final String sqlDeleteEdge = String.format(
				"DELETE FROM %s WHERE id = ?;", DuckDatabase.TABLE_EDGES
			);
			try (PreparedStatement stmt = db.prepareSQL(sqlDeleteEdge)) {
				stmt.setLong(1, id);
				stmt.execute();
			}
		} catch (SQLException e) {
			LOGGER.error("Failed to delete edge " + id, e);
		}
	}

	@Override
	public String toString() {
		return String.format(
			"DuckEdge [edgeId=%s, type=%s, startNodeId=%s, endNodeId=%s]",
			id, type, startNodeId, endNodeId);
	}
	
}
