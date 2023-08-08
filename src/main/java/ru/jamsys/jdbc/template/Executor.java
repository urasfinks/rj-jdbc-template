package ru.jamsys.jdbc.template;

import org.jetbrains.annotations.NotNull;
import ru.jamsys.component.JdbcTemplate;
import ru.jamsys.pool.Pool;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

//Executor был внедрён для разорванных операций в рамках одного коннекта
//Например для начала [for update], после какая-то логика, а потом [commit]

public class Executor {
    private final Pool<Connection> pool;
    private final Connection conn;
    private final JdbcTemplate jdbcTemplate;
    private Exception error;

    public Executor(@NotNull Pool<Connection> pool, JdbcTemplate jdbcTemplate) throws Exception {
        this.pool = pool;
        this.conn = pool.getResource();
        this.jdbcTemplate = jdbcTemplate;
    }

    public void close() throws Exception {
        if (error != null) {
            throw error;
        }
        pool.complete(conn, null);
    }

    public boolean isSuccess() {
        return error == null;
    }

    public List<Map<String, Object>> execute(TemplateEnum templateEnum, Map<String, Object> args) throws Exception {
        return execute(templateEnum, args, false);
    }

    public List<Map<String, Object>> execute(TemplateEnum templateEnum, Map<String, Object> args, boolean debug) throws Exception {
        //Цепочка вызовов может исполняться до первого исключения, иначе смысл цепочки теряется
        if (error != null) {
            throw error;
        }
        try {
            return jdbcTemplate.executeNative(conn, pool, templateEnum, args, debug, false);
        } catch (Exception e) {
            error = e;
            throw e;
        }
    }
}
