package ru.jamsys.jdbc.template;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import ru.jamsys.App;
import ru.jamsys.component.Security;
import ru.jamsys.component.StatisticReaderDefault;
import ru.jamsys.pool.PostgreSQL;

import java.util.HashMap;
import java.util.List;

class TemplateTest {

    @BeforeAll
    static void beforeAll() {
        String[] args = new String[]{};
        App.context = SpringApplication.run(App.class, args);
        App.context.getBean(StatisticReaderDefault.class);
    }

    @Test
    void parseSql2() throws Exception {
        Template template = new Template("""
                SELECT
                    type_data AS key,
                    max(revision_data) AS max
                FROM data WHERE (type_data IN ('%s', '%s', '%s', '%s', '%s', '%s') AND id_user = 1 )
                OR ( type_data IN ('%s', '%s') AND id_user = ${IN.id_user::NUMBER})
                OR ( type_data = '%s' AND (uuid_device_data = ${IN.uuid_device::VARCHAR} OR id_user = ${IN.id_user::NUMBER}))
                AND lazy_sync_data IN (${IN.lazy::IN_ENUM_VARCHAR})
                AND id_user = ${IN.id_user::NUMBER})
                AND lazy_sync_data IN (${IN.lazy::IN_ENUM_VARCHAR})
                AND uuid_device_data = ${IN.uuid_device::VARCHAR}
                GROUP BY type_data;
                """, StatementType.SELECT_WITH_AUTO_COMMIT);
        HashMap<String, Object> args = new HashMap<>();
        args.put("id_user", 1);
        args.put("uuid_device", "a1b2c3");
        args.put("lazy", List.of("Hello", "world", "wfe"));

        CompiledSqlTemplate compiledSqlTemplate = template.compileSqlTemplate(args);
        Assertions.assertEquals("""
                SELECT
                    type_data AS key,
                    max(revision_data) AS max
                FROM data WHERE (type_data IN ('%s', '%s', '%s', '%s', '%s', '%s') AND id_user = 1 )
                OR ( type_data IN ('%s', '%s') AND id_user = ?)
                OR ( type_data = '%s' AND (uuid_device_data = ? OR id_user = ?))
                AND lazy_sync_data IN (?,?,?)
                AND id_user = ?)
                AND lazy_sync_data IN (?,?,?)
                AND uuid_device_data = ?
                GROUP BY type_data;
                """, compiledSqlTemplate.getSql(), "#1");

        Assertions.assertEquals("""
                SELECT
                    type_data AS key,
                    max(revision_data) AS max
                FROM data WHERE (type_data IN ('%s', '%s', '%s', '%s', '%s', '%s') AND id_user = 1 )
                OR ( type_data IN ('%s', '%s') AND id_user = 1)
                OR ( type_data = '%s' AND (uuid_device_data = 'a1b2c3' OR id_user = 1))
                AND lazy_sync_data IN ('Hello','world','wfe')
                AND id_user = 1)
                AND lazy_sync_data IN ('Hello','world','wfe')
                AND uuid_device_data = 'a1b2c3'
                GROUP BY type_data;
                """, template.debugSql(compiledSqlTemplate), "#2");
    }

    @Test
    void parseSql() throws Exception {
        Template template = new Template("select * from table where c1 = ${IN.name::VARCHAR} and c2 = ${IN.name::VARCHAR}", StatementType.SELECT_WITH_AUTO_COMMIT);
        Assertions.assertEquals("select * from table where c1 = ? and c2 = ?", template.getSql(), "#1");
    }

    //@Test
    void realConnection() throws Exception {

        Security security = App.context.getBean(Security.class);
        security.init("12345".toCharArray());
        security.add("test", "changeme".toCharArray());

        PostgreSQL first = new PostgreSQL("First", 1, 10, 60000);
        first.setSecurity(security);
        first.setSecurityKey("test");
        first.initial();

        /*Template template = new Template("select id_srv from srv", StatementType.SELECT);

        JdbcTemplate bean = App.context.getBean(JdbcTemplate.class);
        bean.addPool(first);

        List<Map<String, Object>> exec = bean.exec("First", "srv", null);
        System.out.println(exec);*/

    }

}