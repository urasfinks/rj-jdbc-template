package ru.jamsys.jdbc.template;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import ru.jamsys.App;
import ru.jamsys.component.JdbcTemplate;
import ru.jamsys.component.Security;
import ru.jamsys.component.StatisticReaderDefault;
import ru.jamsys.pool.PostgreSQL;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class TemplateTest {

    @BeforeAll
    static void beforeAll() {
        String[] args = new String[]{};
        App.context = SpringApplication.run(App.class, args);
        App.context.getBean(StatisticReaderDefault.class);
    }

    @Test
    void parseSql() throws Exception {
        Template template = new Template("select * from table where c1 = ${IN.name::VARCHAR} and c2 = ${IN.name::VARCHAR}", StatementType.SELECT);
        Assertions.assertEquals("select * from table where c1 = ? and c2 = ?", template.getSqlStatement(), "#1");
        Assertions.assertEquals("name", String.join(",", template.getArgs().keySet().toArray(new String[0])), "#2");
        Assertions.assertEquals("1,2", Arrays.stream(template.getArgs().get("name").getIndex().toArray(new Integer[0])).map(Object::toString).collect(Collectors.joining(",")), "#3");

        template = new Template("select * from table where c1 = ${IN.name::VARCHAR} and c2 = ${name}", StatementType.SELECT);
        Assertions.assertEquals("select * from table where c1 = ? and c2 = ?", template.getSqlStatement(), "#4");
        Assertions.assertEquals("name", String.join(",", template.getArgs().keySet().toArray(new String[0])), "#5");
        Assertions.assertEquals("1,2", Arrays.stream(template.getArgs().get("name").getIndex().toArray(new Integer[0])).map(Object::toString).collect(Collectors.joining(",")), "#6");

    }

    @Test
    void realConnection() throws Exception {
        Security security = App.context.getBean(Security.class);
        security.add("test", "changeme");

        PostgreSQL first = new PostgreSQL("First", 1, 10, 60000);
        first.setSecurity(security);
        first.setSecurityKey("test");
        first.initial();

        Template template = new Template("select id_srv from srv", StatementType.SELECT);

        JdbcTemplate bean = App.context.getBean(JdbcTemplate.class);
        bean.addPool(first);
        bean.addTemplate("srv", template);

        List<Map<String, Object>> exec = bean.exec("First", "srv", null);
        System.out.println(exec);

    }

}