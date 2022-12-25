package ru.jamsys.pool;

import lombok.Getter;
import lombok.Setter;
import ru.jamsys.component.Security;
import ru.jamsys.jdbc.template.DefaultStatementControl;
import ru.jamsys.jdbc.template.StatementControl;
import ru.jamsys.pool.PoolJdbc;

import java.sql.Connection;
import java.sql.DriverManager;

public class PostgreSQL extends AbstractPool<Connection> implements PoolJdbc {

    @Setter
    private String uri = "jdbc:postgresql://127.0.0.1:5432/postgres";
    @Setter
    private String user = "postgres";
    @Setter
    private String securityKey = "";
    @Setter
    private Security security;

    @Getter
    private StatementControl statementControl = new DefaultStatementControl();

    public PostgreSQL(String name, int min, int max, long keepAliveMillis) {
        super(name, min, max, keepAliveMillis);
    }

    @Override
    public Connection createResource() {
        try {
            return DriverManager.getConnection(uri, user, security.get(securityKey));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void closeResource(Connection resource) {

    }

    @Override
    public boolean checkExceptionOnRemove(Exception e) {
        return false;
    }

}
