package ru.jamsys.pool;

import lombok.Getter;
import lombok.Setter;
import ru.jamsys.component.Security;
import ru.jamsys.jdbc.template.DefaultStatementControl;
import ru.jamsys.jdbc.template.StatementControl;

import java.sql.Connection;
import java.sql.DriverManager;

public class PostgreSQL extends AbstractPool<Connection> implements PoolJdbc {

    @Setter
    private String uri = "jdbc:postgresql://127.0.0.1:5432/postgres";
    @Setter
    private String user = "";
    @Setter
    private String securityKey = "";
    @Setter
    private Security security;

    @Getter
    private final StatementControl statementControl = new DefaultStatementControl();

    public PostgreSQL(String name, int min, int max, long keepAliveMillis) {
        super(name, min, max, keepAliveMillis);
    }

    @SuppressWarnings("unused")
    public void initial(String uri, String user, Security security, String securityKey) {
        this.uri = uri;
        this.user = user;
        this.security = security;
        this.securityKey = securityKey;
        super.initial();
    }

    @Override
    public Connection createResource() {
        try {
            return DriverManager.getConnection(uri, user, new String(security.get(securityKey)));
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
