package ru.jamsys.jdbc.template;

import java.sql.*;

public class DefaultStatementControl implements StatementControl {

    @Override
    public void setInClob(Connection connection, PreparedStatement cs, int index, String value) throws SQLException {
        cs.setString(index, value);
    }

    @Override
    public void setOutClob(CallableStatement cs, int index) throws SQLException {
        cs.registerOutParameter(index, Types.VARCHAR);
    }

    @Override
    public Object getOutClob(CallableStatement cs, int index) throws SQLException {
        return cs.getString(index);
    }

    @Override
    public Object getColumnClob(ResultSet cs, String name) throws SQLException {
        return cs.getString(name);
    }

}
