package org.eclipse.hawk.duckdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.eclipse.hawk.core.graph.IGraphDatabase;
import org.eclipse.hawk.core.graph.IGraphEdge;
import org.eclipse.hawk.core.graph.IGraphNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuckNode implements IGraphNode {

	public enum ValueType {
		BOOLEAN {
			@Override
			String getColumnName() {
				return "value_boolean";
			}

			@Override
			void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
				stmt.setBoolean(index, (boolean) value);
			}

			@Override
			String getColumnType() {
				return "BOOLEAN";
			}

			@Override
			Object getValue(ResultSet rs, int index) throws SQLException {
				boolean b = rs.getBoolean(index);
				return rs.wasNull() ? null : b;
			}
		}, LONG {
			@Override
			String getColumnName() {
				return "value_long";
			}

			@Override
			void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
				stmt.setLong(index, ((Number) value).longValue());
			}

			@Override
			String getColumnType() {
				return "BIGINT";
			}

			@Override
			Object getValue(ResultSet rs, int index) throws SQLException {
				long b = rs.getLong(index);
				return rs.wasNull() ? null : b;
			}
		}, FLOAT {
			@Override
			String getColumnName() {
				return "value_float";
			}

			@Override
			void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
				stmt.setDouble(index, ((Number) value).floatValue());
			}

			@Override
			String getColumnType() {
				return "FLOAT";
			}

			@Override
			Object getValue(ResultSet rs, int index) throws SQLException {
				float b = rs.getFloat(index);
				return rs.wasNull() ? null : b;
			}
		}, DOUBLE {
			@Override
			String getColumnName() {
				return "value_double";
			}

			@Override
			void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
				stmt.setDouble(index, ((Number) value).doubleValue());
			}

			@Override
			String getColumnType() {
				return "DOUBLE";
			}

			@Override
			Object getValue(ResultSet rs, int index) throws SQLException {
				double b = rs.getDouble(index);
				return rs.wasNull() ? null : b;
			}
		}, STRING {
			@Override
			String getColumnName() {
				return "value_string";
			}

			@Override
			void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
				stmt.setString(index, (String)value);
			}

			@Override
			String getColumnType() {
				return "VARCHAR";
			}

			@Override
			Object getValue(ResultSet rs, int index) throws SQLException {
				return rs.getString(index);
			}
		}, BLOB {
			@Override
			String getColumnName() {
				return "value_blob";
			}
			
			@Override
			String getColumnExpression() {
				// TODO GABOR: workaround over the fact that DuckDB's JDBC backend does not allow for directly SELECTing BLOB columns
				return "CAST(value_blob AS VARCHAR)";
			}

			@Override
			void setParameter(PreparedStatement stmt, int index, Object value)
				throws SQLException, IOException {
				try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
					 ObjectOutputStream oos = new ObjectOutputStream(bos)) {
					oos.writeObject(value);
					final String hexString = "\\x" + Hex.encodeHexString(bos.toByteArray());
					stmt.setString(index, hexString);
				}
			}			

			@Override
			String getColumnType() {
				return "BLOB";
			}

			@Override
			Object getValue(ResultSet rs, int index) throws SQLException, IOException, ClassNotFoundException {
				String s = rs.getString(index);
				if (s == null) { 
					return null;
				}
				
				try {
					// Remove '\x' header from DuckDB and decode
					byte[] bytes = Hex.decodeHex(s.substring(2).toCharArray());

					try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
							 ObjectInputStream ois = new ObjectInputStream(bis)) {
						return ois.readObject();
					}
				} catch (DecoderException e) {
					throw new IOException(e);
				}
			}
		};
		
		abstract String getColumnName();
		abstract String getColumnType();
		String getColumnExpression() {
			return getColumnName();
		}

		abstract Object getValue(ResultSet rs, int index) throws SQLException, IOException, ClassNotFoundException;
		
		abstract void setParameter(PreparedStatement stmt, int index, Object value)
			throws SQLException, IOException;
		
		public static ValueType from(Object value) {
			if (value instanceof Boolean) {
				return BOOLEAN;
			} else if (value instanceof Float) {
				return FLOAT;
			} else if (value instanceof Double) {
				return DOUBLE;
			} else if (value instanceof Number) {
				return LONG;
			} else if (value instanceof String) {
				return STRING;
			} else {
				return BLOB;
			}
		}

		public static String sqlTableColumns() {
			StringBuffer sbuf = new StringBuffer();
			boolean first = true;
			for (ValueType vt : values()) {
				if (first) {
					first = false;
				} else {
					sbuf.append(", ");
				}

				sbuf.append(vt.getColumnName());
				sbuf.append(' ');
				sbuf.append(vt.getColumnType());
			}
			return sbuf.toString();
		}

		public static String sqlQueryColumns() {
			StringBuffer sbuf = new StringBuffer();
			boolean first = true;
			for (ValueType vt : values()) {
				if (first) {
					first = false;
				} else {
					sbuf.append(", ");
				}
				
				sbuf.append(vt.getColumnExpression());
			}
			return sbuf.toString();
		}
	}
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DuckNode.class);

	private final long nodeId;
	private final Connection duckDB;

	public DuckNode(Connection duckDB, long nodeId) {
		this.duckDB = duckDB;
		this.nodeId = nodeId;
	}

	@Override
	public Object getId() {
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
			ValueType.sqlQueryColumns(),
			DuckDatabase.TABLE_PROPERTIES);

		try (PreparedStatement vq = duckDB.prepareStatement(valueQuery)) {
			vq.setLong(1, nodeId);
			vq.setString(2, name);

			ResultSet rs = vq.executeQuery();
			if (rs.next()) {
				int i = 0;
				for (ValueType vt : ValueType.values()) {
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

		final ValueType vt = ValueType.from(value);
		final String updateQuery = String.format(
			"UPDATE %s SET %s = ?%s WHERE node_id = ? AND name = ?;",
			DuckDatabase.TABLE_PROPERTIES, vt.getColumnName(), vt == ValueType.BLOB ? "::BLOB" : "");

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
		// TODO Auto-generated method stub

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
