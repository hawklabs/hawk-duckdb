package org.eclipse.hawk.duckdb;

import java.sql.Connection;
import java.util.Objects;
import java.util.Set;

import org.eclipse.hawk.core.graph.IGraphEdge;
import org.eclipse.hawk.core.graph.IGraphNode;

/**
 * Represents a single edge, as stored in DuckDB. Mutable state is not kept in the object:
 * instead, it is always queried on the fly from DuckDB.
 */
public class DuckEdge implements IGraphEdge {

	private final Connection duckDB;
	private final long edgeId;
	private final String type;
	private final long startNodeId;
	private final long endNodeId;

	public DuckEdge(Connection duckDB, long id, String type, long startNodeId, long endNodeId) {
		this.duckDB = duckDB;
		this.edgeId = id;
		this.type = type;
		this.startNodeId = startNodeId;
		this.endNodeId = endNodeId;
	}

	@Override
	public Long getId() {
		return edgeId;
	}

	@Override
	public String getType() {
		return type;
	}

	@Override
	public Set<String> getPropertyKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getProperty(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setProperty(String name, Object value) {
		// TODO Auto-generated method stub

	}

	@Override
	public IGraphNode getStartNode() {
		return new DuckNode(duckDB, startNodeId);
	}

	@Override
	public IGraphNode getEndNode() {
		return new DuckNode(duckDB, endNodeId);
	}

	@Override
	public void delete() {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeProperty(String name) {
		// TODO Auto-generated method stub

	}

	@Override
	public int hashCode() {
		return Objects.hash(edgeId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DuckEdge other = (DuckEdge) obj;
		return edgeId == other.edgeId;
	}

	@Override
	public String toString() {
		return String.format(
			"DuckEdge [edgeId=%s, type=%s, startNodeId=%s, endNodeId=%s]",
			edgeId, type, startNodeId, endNodeId);
	}
	
}
