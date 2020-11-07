package org.eclipse.hawk.duckdb;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.eclipse.hawk.core.graph.IGraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for nodes and edges, which share the ability to store properties.
 */
public class AbstractDuckElement {

	private static final Logger LOGGER = LoggerFactory.getLogger(DuckNode.class);

	protected final long id;
	protected final DuckDatabase db;

	public AbstractDuckElement(DuckDatabase db, long id) {
		this.db = db;
		this.id = id;
	}

	public Long getId() {
		return id;
	}
	
	public IGraphDatabase getGraph() {
		return db;
	}
	
	public Set<String> getPropertyKeys() {
		String sqlQuery = String.format(
			"SELECT name FROM %s WHERE elem_id = ?;",
			DuckDatabase.TABLE_PROPERTIES
		);
		
		try (PreparedStatement query = db.duckDB.prepareStatement(sqlQuery)) {
			query.setLong(1, id);
	
			ResultSet rs = query.executeQuery();
			Set<String> keys = new HashSet<>();
			while (rs.next()) {
				keys.add(rs.getString(1));
			}
			return keys;
		} catch (SQLException e) {
			LOGGER.error("Could not list the property keys for node " + id, e);
			return Collections.emptySet();
		}
	}

	public Object getProperty(String name) {
		final String valueQuery = String.format(
			"SELECT %s FROM %s WHERE elem_id = ? AND name = ?;",
			PropertyValueType.sqlQueryColumns(),
			DuckDatabase.TABLE_PROPERTIES);
	
		try (PreparedStatement vq = db.duckDB.prepareStatement(valueQuery)) {
			vq.setLong(1, id);
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
			LOGGER.error("Could not fetch property " + name + " on node " + id, e);
		}
		
		return null;
	}

	public void setProperty(String name, Object value) {
		if (value == null) {
			removeProperty(name);
			return;
		}
	
		final PropertyValueType vt = PropertyValueType.from(value);
		final String updateQuery = String.format(
			"UPDATE %s SET %s = ?%s WHERE elem_id = ? AND name = ?;",
			DuckDatabase.TABLE_PROPERTIES, vt.getColumnName(), vt == PropertyValueType.BLOB ? "::BLOB" : "");
	
		try (PreparedStatement update = db.duckDB.prepareStatement(updateQuery)) {
			// TODO GABOR: I have to put the new value as the *third* element in
			// the UPDATE, or I get conversion errors mentioning the wrong value
			// - this looks like a bug in the JDBC driver?
			vt.setParameter(update, 3, value);
			update.setLong(1, id);
			update.setString(2, name);

			final int rowsChanged = update.executeUpdate();
			if (rowsChanged == 0) {
				// No rows were updated: do an insert
				final String insertQuery = String.format(
					"INSERT INTO %s (elem_id, name, %s) VALUES (?, ?, ?);",
					DuckDatabase.TABLE_PROPERTIES, vt.getColumnName());
	
				try (PreparedStatement insert = db.duckDB.prepareStatement(insertQuery)) {
					insert.setLong(1, id);
					insert.setString(2, name);
					vt.setParameter(insert, 3, value);
					insert.executeUpdate();
				}
			}
		} catch (SQLException | IOException e) {
			LOGGER.error("Could not update property " + name + " on node " + id, e);
		}
	}

	public void removeProperty(String name) {
		final String sqlDelete = String.format(
			"DELETE FROM %s WHERE elem_id = ? AND name = ?;",
			DuckDatabase.TABLE_PROPERTIES
		);
	
		try (PreparedStatement stmt = db.duckDB.prepareStatement(sqlDelete)) {
			stmt.setLong(1, id);
			stmt.setString(2, name);
			stmt.execute();
		} catch (SQLException e) {
			LOGGER.error("Could not remove property " + name + " from node " + id, e);
		}
	}


	protected void deleteProperties() throws SQLException {
		String sqlDeleteProps = String.format(
			"DELETE FROM %s WHERE elem_id = ?;",
			DuckDatabase.TABLE_PROPERTIES
		);
		try (PreparedStatement stmt = db.duckDB.prepareStatement(sqlDeleteProps)) {
			stmt.setLong(1, id);
			stmt.execute();
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AbstractDuckElement other = (AbstractDuckElement) obj;
		return id == other.id;
	}
	
}