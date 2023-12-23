package ru.jamsys.jdbc.template;

import lombok.Data;
import ru.jamsys.template.TemplateItem;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class Template {

    private String cachedSql;
    private final StatementType statementType;
    private boolean dynamicArgument = false;
    private List<TemplateItem> listTemplateItem;
    private List<Argument> listArgument;

    public Template(String sql, StatementType statementType) throws Exception {
        this.statementType = statementType;
        parseSql(sql);
    }

    public String getSql() {
        try {
            CompiledSqlTemplate compiledSqlTemplate = compileSqlTemplate(new HashMap<>());
            return compiledSqlTemplate.getSql();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void parseSql(String sql) throws Exception {
        listTemplateItem = ru.jamsys.template.Template.getParsedTemplate(sql);
        listArgument = new ArrayList<>();
        for (TemplateItem templateItem : listTemplateItem) {
            if (!templateItem.isStatic) {
                Argument argument = new Argument();
                argument.parseSqlKey(templateItem.value);
                listArgument.add(argument);
                if (argument.getType().isDynamicFragment()) {
                    dynamicArgument = true;
                }
            }
        }
        analyze();
    }

    public void analyze() throws Exception {
        for (Argument argument : listArgument) {
            if (statementType.isSelect()
                    && (
                    argument.getDirection() == ArgumentDirection.OUT
                            || argument.getDirection() == ArgumentDirection.IN_OUT
            )) {
                throw new Exception("Нельзя использовать OUT переменные в простых выборках");
            }
        }
    }

    @SuppressWarnings("unused")
    public static void setParam(
            StatementControl statementControl,
            Connection conn,
            PreparedStatement preparedStatement,
            Argument arg
    ) throws Exception {
        switch (arg.getDirection()) {
            case IN ->
                    statementControl.setInParam(conn, preparedStatement, arg.getType(), arg.getIndex(), arg.getValue());
            case OUT ->
                    statementControl.setOutParam((CallableStatement) preparedStatement, arg.getType(), arg.getIndex());
            case IN_OUT -> {
                statementControl.setOutParam((CallableStatement) preparedStatement, arg.getType(), arg.getIndex());
                statementControl.setInParam(conn, preparedStatement, arg.getType(), arg.getIndex(), arg.getValue());
            }
        }
    }

    public CompiledSqlTemplate compileSqlTemplate(Map<String, Object> args) throws CloneNotSupportedException {
        CompiledSqlTemplate compiledSqlTemplate = new CompiledSqlTemplate();
        Map<String, String> templateArgs = new HashMap<>();
        int newIndex = 1;
        for (Argument argument : listArgument) {
            ArgumentType argumentType = argument.getType();
            Object argumentValue = args.get(argument.getKey());
            List<Argument> resultListArgument = compiledSqlTemplate.getListArgument();
            if (argumentType.isDynamicFragment()) {
                templateArgs.put(
                        argument.getSqlKeyTemplate(),
                        argumentType.compileDynamicFragment(argumentValue, argument.key)
                );
                for (Object obj : (List) argumentValue) {
                    Argument clone = argument.clone();
                    clone.setValue(obj);
                    clone.setIndex(newIndex++);
                    clone.setType(clone.getType().getRealType());
                    resultListArgument.add(clone);
                }
            } else {
                templateArgs.put(argument.getSqlKeyTemplate(), "?");
                Argument clone = argument.clone();
                clone.setValue(argumentValue);
                clone.setIndex(newIndex++);
                resultListArgument.add(clone);
            }
        }
        if (cachedSql == null && !dynamicArgument) {
            cachedSql = ru.jamsys.template.Template.template(listTemplateItem, templateArgs);
        }
        if (dynamicArgument) {
            compiledSqlTemplate.setSql(ru.jamsys.template.Template.template(listTemplateItem, templateArgs));
        } else {
            compiledSqlTemplate.setSql(cachedSql);
        }
        return compiledSqlTemplate;
    }

    public String debugSql(CompiledSqlTemplate compiledSqlTemplate) {
        String[] split = compiledSqlTemplate.getSql().split("\\?");
        StringBuilder sb = new StringBuilder();
        List<Argument> listArgument = compiledSqlTemplate.getListArgument();
        for (int i = 0; i < split.length; i++) {
            sb.append(split[i]);
            try {
                Argument argument = listArgument.get(i);
                String value = argument.getValue().toString();
                if (argument.type.isString()) {
                    value = "'" + value + "'";
                }
                sb.append(value);
            } catch (Exception e) {
            }
        }
        return sb.toString();
    }

    public static List<Map<String, Object>> execute(
            Connection conn,
            Template template,
            Map<String, Object> args,
            StatementControl statementControl,
            boolean debug
    ) throws Exception {
        CompiledSqlTemplate compiledSqlTemplate = template.compileSqlTemplate(args);
        if (debug) {
            System.out.println(compiledSqlTemplate.getSql());
            System.out.println(template.debugSql(compiledSqlTemplate));
        }
        StatementType statementType = template.getStatementType();
        conn.setAutoCommit(statementType.isAutoCommit());
        PreparedStatement preparedStatement =
                statementType.isSelect()
                        ? conn.prepareStatement(compiledSqlTemplate.getSql())
                        : conn.prepareCall(compiledSqlTemplate.getSql());
        for (Argument argument : compiledSqlTemplate.getListArgument()) {
            setParam(statementControl, conn, preparedStatement, argument);
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
                for (Argument argument : template.listArgument) {
                    ArgumentDirection direction = argument.getDirection();
                    if (direction == ArgumentDirection.OUT || direction == ArgumentDirection.IN_OUT) {
                        row.put(
                                argument.getKey(),
                                statementControl.getOutParam(
                                        (CallableStatement) preparedStatement,
                                        argument.getType(),
                                        argument.getIndex())
                        );
                    }
                }
                listRet.add(row);
                break;
        }
        return listRet;
    }

}
