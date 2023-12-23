package ru.jamsys.component;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import ru.jamsys.AbstractCoreComponent;

import ru.jamsys.jdbc.template.Executor;
import ru.jamsys.jdbc.template.Template;
import ru.jamsys.jdbc.template.TemplateEnum;
import ru.jamsys.pool.*;
import ru.jamsys.scheduler.SchedulerGlobal;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Lazy
public class JdbcTemplate extends AbstractCoreComponent {

    @Getter
    @Value("${rj.jdbcTemplate.uri:jdbc:postgresql://127.0.0.1:5432/postgres}")
    private String uri;

    @Getter
    @Value("${rj.jdbcTemplate.user:postgres}")
    private String user;

    @Getter
    @Value("${rj.jdbcTemplate.securityKey:postgresql_server}")
    private String securityKey;

    final String nameSchedulerStabilizer = "SchedulerJdbcStabilizer";
    private final Scheduler scheduler;
    private final StatisticAggregator statisticAggregator;
    private final Map<String, Pool<Connection>> mapPool = new ConcurrentHashMap<>();

    public JdbcTemplate(StatisticAggregator statisticAggregator, Scheduler scheduler) {
        this.scheduler = scheduler;
        this.statisticAggregator = statisticAggregator;
        scheduler.add(nameSchedulerStabilizer, this::stabilizer, 1000);
        scheduler.add(SchedulerGlobal.SCHEDULER_GLOBAL_STATISTIC_WRITE, this::flushStatistic);
    }

    @SuppressWarnings("unused")
    public void addPool(Pool<Connection> pool) {
        mapPool.put(pool.getName(), pool);
    }

    @SuppressWarnings("unused")
    public Map<String, Object> createArguments() {
        return new HashMap<>();
    }

    @SuppressWarnings("unused")
    public Executor getExecutor(String namePool) throws Exception {
        return new Executor(mapPool.get(namePool), this);
    }

    public List<Map<String, Object>> execute(String namePool, TemplateEnum templateEnum, Map<String, Object> args, boolean debug) throws Exception {
        Pool<Connection> pool = mapPool.get(namePool);
        Connection res = pool.getResource();
        return executeNative(res, pool, templateEnum, args, debug, true);
    }

    public List<Map<String, Object>> executeNative(Connection res, Pool<Connection> pool, TemplateEnum templateEnum, Map<String, Object> args, boolean debug, boolean controlPool) throws Exception {
        Template template = templateEnum.getTemplate();
        if (template == null) {
            pool.complete(res, null);
            throw new Exception("TemplateEnum: " + templateEnum + " return null template");
        }
        List<Map<String, Object>> execute;
        try {
            execute = Template.execute(res, template, args, ((PoolJdbc) pool).getStatementControl() ,debug);
            if (controlPool) {
                pool.complete(res, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            pool.complete(res, e);
            throw e;
        }
        return execute;
    }

    public List<Map<String, Object>> execute(String namePool, TemplateEnum templateEnum, Map<String, Object> args) throws Exception {
        return execute(namePool, templateEnum, args, false);
    }

    @SuppressWarnings("unused")
    public long addIfNotExist(String namePool, TemplateEnum selectSql, TemplateEnum insertSql, String idField, Map<String, Object> args) throws Exception {
        List<Map<String, Object>> select = execute(namePool, selectSql, args);
        Long id;
        if (!select.isEmpty()) {
            id = (Long) select.get(0).get(idField);
        } else {
            List<Map<String, Object>> insertWord = execute(namePool, insertSql, args);
            id = (Long) insertWord.get(0).get(idField);
        }
        return id;
    }

    @Override
    public void flushStatistic() {
        String[] strings = mapPool.keySet().toArray(new String[0]);
        PoolAggregateStatisticData aggStat = new PoolAggregateStatisticData();
        for (String name : strings) {
            Pool<Connection> threadBalancer = mapPool.get(name);
            if (threadBalancer != null) {
                PoolStatisticData thStat = threadBalancer.flushStatistic();
                if (thStat != null) {
                    aggStat.getMap().put(name, thStat.clone());
                }
            }
        }
        statisticAggregator.add(aggStat);
    }

    private void stabilizer() {
        String[] strings = mapPool.keySet().toArray(new String[0]);
        for (String name : strings) {
            Pool<Connection> pool = mapPool.get(name);
            if (pool != null) {
                pool.stabilizer();
            }
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        String[] strings = mapPool.keySet().toArray(new String[0]);
        for (String key : strings) {
            mapPool.remove(key).shutdown();
        }
        scheduler.remove(nameSchedulerStabilizer, this::stabilizer);
        scheduler.remove(SchedulerGlobal.SCHEDULER_GLOBAL_STATISTIC_WRITE, this::flushStatistic);
    }

}
