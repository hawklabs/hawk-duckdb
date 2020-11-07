package org.eclipse.hawk.duckdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * Types of values that a node or edge property can be set to.
 */
public enum PropertyValueType {
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
	},  INTEGER {
		@Override
		String getColumnName() {
			return "value_int";
		}

		@Override
		void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
			stmt.setInt(index, ((Number) value).intValue());
		}

		@Override
		String getColumnType() {
			return "INTEGER";
		}

		@Override
		Object getValue(ResultSet rs, int index) throws SQLException {
			int b = rs.getInt(index);
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
	
	public static PropertyValueType from(Object value) {
		if (value instanceof Boolean) {
			return BOOLEAN;
		} else if (value instanceof Float) {
			return FLOAT;
		} else if (value instanceof Double) {
			return DOUBLE;
		} else if (value instanceof Long) {
			return LONG;
		} else if (value instanceof Number) {
			return INTEGER;
		} else if (value instanceof String) {
			return STRING;
		} else {
			return BLOB;
		}
	}

	public static String sqlTableColumns() {
		StringBuffer sbuf = new StringBuffer();
		boolean first = true;
		for (PropertyValueType vt : values()) {
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
		for (PropertyValueType vt : values()) {
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