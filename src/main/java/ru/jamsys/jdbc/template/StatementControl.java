package ru.jamsys.jdbc.template;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public interface StatementControl {

	default void setInParam(Connection connection, PreparedStatement cs, ArgumentType type, int index, Object value) throws Exception {
		boolean isNull = value == null;
		if (isNull) {
			setInNull(cs, index, type);
			return;
		}

		String valueStr = String.valueOf(value);
		switch (type) {
			case VARCHAR:
				setInVarchar(cs, index, valueStr);
				break;
			case CLOB:
				setInClob(connection, cs, index, valueStr);
				break;
			case NUMBER:
				setInNumber(cs, index, valueStr);
				break;
			case TIMESTAMP:
				setInTimestamp(cs, index, valueStr);
				break;
		}
	}

	default void setInNull(PreparedStatement cs, int index, ArgumentType type) throws SQLException {
		int sqlType = -1;
		switch (type) {
			case VARCHAR:
				sqlType = Types.VARCHAR;
				break;
			case NUMBER:
				sqlType = Types.NUMERIC;
				break;
			case TIMESTAMP:
				sqlType = Types.TIMESTAMP;
				break;
			case CLOB:
				sqlType = Types.CLOB;
				break;
		}
		cs.setNull(index, sqlType);
	}

	default void setInVarchar(PreparedStatement cs, int index, String value) throws SQLException {
		cs.setString(index, value);
	}

	void setInClob(Connection connection, PreparedStatement cs, int index, String value) throws Exception;

	default void setInNumber(PreparedStatement cs, int index, String value) throws SQLException {
		cs.setBigDecimal(index, value != null && !value.isEmpty() ? new BigDecimal(value) : null);
	}

	default void setInTimestamp(PreparedStatement cs, int index, String value) throws SQLException {
		boolean condition = (value != null) && !value.isEmpty();
		if (!condition) {
			cs.setTimestamp(index, null);
			return;
		}
		try {
			long timestampMills = Long.parseLong(value);
			LocalDateTime timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestampMills), ZoneId.systemDefault());
			cs.setTimestamp(index, Timestamp.valueOf(timestamp));
		} catch (NumberFormatException e) {
			cs.setTimestamp(index, Timestamp.valueOf(ZonedDateTime.parse(value).toLocalDateTime()));
		}
	}

	default void setOutParam(CallableStatement cs, ArgumentType type, int index) throws SQLException {
		switch (type) {
			case VARCHAR:
				setOutVarchar(cs, index);
				break;
			case CLOB:
				setOutClob(cs, index);
				break;
			case NUMBER:
				setOutNumber(cs, index);
				break;
			case TIMESTAMP:
				setOutTimestamp(cs, index);
				break;
		}
	}

	default void setOutVarchar(CallableStatement cs, int index) throws SQLException {
		cs.registerOutParameter(index, Types.VARCHAR);
	}

	void setOutClob(CallableStatement cs, int index) throws SQLException;

	default void setOutNumber(CallableStatement cs, int index) throws SQLException {
		cs.registerOutParameter(index, Types.NUMERIC);
	}

	default void setOutTimestamp(CallableStatement cs, int index) throws SQLException {
		cs.registerOutParameter(index, Types.TIMESTAMP);
	}

	default Object getOutParam(CallableStatement cs, ArgumentType type, int index) throws Exception {
		switch (type) {
			case VARCHAR:
				return getOutVarchar(cs, index);
			case CLOB:
				return getOutClob(cs, index);
			case NUMBER:
				return getOutNumber(cs, index);
			case TIMESTAMP:
				return getOutTimestamp(cs, index);
		}
		return null;
	}

	default String getOutVarchar(CallableStatement cs, int index) throws SQLException {
		return cs.getString(index);
	}

	Object getOutClob(CallableStatement cs, int index) throws Exception;

	default BigDecimal getOutNumber(CallableStatement cs, int index) throws SQLException {
		return cs.getBigDecimal(index);
	}

	default Timestamp getOutTimestamp(CallableStatement cs, int index) throws SQLException {
		return cs.getTimestamp(index);
	}

	@SuppressWarnings("unused")
	default Object getColumn(ResultSet rs, ArgumentType type, String name) throws Exception {
		switch (type) {
			case VARCHAR:
				return getColumnVarchar(rs, name);
			case CLOB:
				return getColumnClob(rs, name);
			case NUMBER:
				return getColumnNumber(rs, name);
			case TIMESTAMP:
				return getColumnTimestamp(rs, name);
		}
		return null;
	}

	default String getColumnVarchar(ResultSet rs, String name) throws SQLException {
		return rs.getString(name);
	}

	Object getColumnClob(ResultSet cs, String name) throws Exception;

	default BigDecimal getColumnNumber(ResultSet rs, String name) throws SQLException {
		return rs.getBigDecimal(name);
	}

	default Timestamp getColumnTimestamp(ResultSet rs, String name) throws SQLException {
		return rs.getTimestamp(name);
	}
	
}
