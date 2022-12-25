package ru.jamsys.pool;

import ru.jamsys.jdbc.template.StatementControl;

public interface PoolJdbc {

    StatementControl getStatementControl();

}
