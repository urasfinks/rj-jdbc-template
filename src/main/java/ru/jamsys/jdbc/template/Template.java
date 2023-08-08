package ru.jamsys.jdbc.template;

import lombok.Data;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class Template {

    private String sqlStatement;
    private Map<String, Argument> args = new HashMap<>();
    private final StatementType statementType;

    public Template(String sql, StatementType statementType) throws Exception {
        this.statementType = statementType;
        parseSql(sql);
    }

    public void parseSql(String sql) throws Exception {
        String[] exp = sql.split("\\$\\{");
        int idx = 1;
        for (String exp_item : exp) {
            if (!exp_item.contains("}")) {
                continue;
            }
            String[] exp2 = exp_item.split("}");
            if (exp2.length <= 0) {
                continue;
            }
            if (args.containsKey(exp2[0])) {
                args.get(exp2[0]).getIndex().add(idx++);
                sql = sql.replace("${" + exp2[0] + "}", "?");
                continue;
            }
            String[] option = exp2[0].split("::");
            if (option.length == 1) {
                throw new Exception("Не достаточно описания для " + exp2[0] + "; Должно быть ${direction.var::type}");
            }

            String[] names = option[0].split("\\.");
            String direction = names[0];
            String name = names[1];
            String type = option[1];

            if (!args.containsKey(name)) {
                Argument argument = new Argument();
                argument.setDirection(ArgumentDirection.valueOf(direction));
                argument.setType(ArgumentType.valueOf(type));
                args.put(name, argument);
            }
            args.get(name).getIndex().add(idx++);
            sql = sql.replace("${" + exp2[0] + "}", "?");
        }
        analyze();
        sqlStatement = sql;
    }

    public void analyze() throws Exception {
        for (String name : args.keySet()) {
            Argument argument = args.get(name);
            if (statementType.isSelect()
                    && (
                    argument.getDirection() == ArgumentDirection.OUT
                            || argument.getDirection() == ArgumentDirection.IN_OUT
            )) {
                throw new Exception("Нельзя использовать OUT переменные в простых выборках");
            }
        }
    }

    public static List<Map<String, Object>> execute(Connection conn, Template template, Map<String, Object> args, StatementControl statementControl) throws Exception {
        StatementType statementType = template.getStatementType();
        conn.setAutoCommit(statementType.isAutoCommit());
        PreparedStatement preparedStatement =
                statementType.isSelect()
                        ? conn.prepareStatement(template.getSqlStatement())
                        : conn.prepareCall(template.getSqlStatement());

        Map<String, Argument> argsTemplate = template.getArgs();
        for (String name : argsTemplate.keySet()) {
            Argument argument = argsTemplate.get(name);
            for (Integer index : argument.getIndex()) {
                switch (argument.getDirection()) {
                    case IN:
                        statementControl.setInParam(conn, preparedStatement, argument.getType(), index, args.get(name));
                        break;
                    case OUT:
                        statementControl.setOutParam((CallableStatement) preparedStatement, argument.getType(), index);
                        break;
                    case IN_OUT:
                        statementControl.setOutParam((CallableStatement) preparedStatement, argument.getType(), index);
                        statementControl.setInParam(conn, preparedStatement, argument.getType(), index, args.get(name));
                        break;
                }
            }
        }
        preparedStatement.execute();
        List<Map<String, Object>> listRet = new ArrayList<>();
        switch (template.getStatementType()) {
            case SELECT_WITHOUT_AUTO_COMMIT:
            case SELECT_WITH_AUTO_COMMIT:
                try (ResultSet resultSet = preparedStatement.getResultSet()) {
                    if (resultSet == null) {
                        return listRet;
                    }
                    Integer columnCount = null;
                    Map<Integer, String> cacheName = new HashMap<>();
                    while (resultSet.next()) {
                        Map<String, Object> row = new HashMap<>();
                        if (columnCount == null) {
                            ResultSetMetaData metaData = resultSet.getMetaData();
                            columnCount = metaData.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                String name = metaData.getColumnName(i);
                                row.put(name, resultSet.getObject(i));
                                cacheName.put(i, name);
                            }
                        } else {
                            for (int i = 1; i <= columnCount; i++) {
                                row.put(cacheName.get(i), resultSet.getObject(i));
                            }
                        }
                        listRet.add(row);
                    }
                }
                break;
            case CALL_WITHOUT_AUTO_COMMIT:
            case CALL_WITH_AUTO_COMMIT:
                Map<String, Object> row = new HashMap<>();
                for (String name : argsTemplate.keySet()) {
                    Argument argument = argsTemplate.get(name);
                    ArgumentDirection direction = argument.getDirection();
                    if (direction == ArgumentDirection.OUT || direction == ArgumentDirection.IN_OUT) {
                        row.put(name, statementControl.getOutParam((CallableStatement) preparedStatement, argument.getType(), argument.getIndex().get(0)));
                    }
                }
                listRet.add(row);
                break;
        }

        return listRet;
    }

}
