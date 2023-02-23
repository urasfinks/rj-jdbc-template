package ru.jamsys.component;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import ru.jamsys.AbstractCoreComponent;

import ru.jamsys.jdbc.template.Template;
import ru.jamsys.jdbc.template.TemplateEnum;
import ru.jamsys.pool.PoolJdbc;
import ru.jamsys.pool.Pool;
import ru.jamsys.pool.PoolAggregateStatisticData;
import ru.jamsys.pool.PoolStatisticData;
import ru.jamsys.scheduler.SchedulerGlobal;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Lazy
public class JdbcTemplate extends AbstractCoreComponent {

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

    public List<Map<String, Object>> exec(String namePool, TemplateEnum templateEnum, Map<String, Object> args, boolean debug) throws Exception {
        Pool<Connection> pool = mapPool.get(namePool);
        Template template = templateEnum.getTemplate();
        if (template == null) {
            throw new Exception("TemplateEnum: " + templateEnum.toString() + " return null template");
        }
        Connection res = pool.getResource();
        List<Map<String, Object>> execute = null;
        if (debug) {
            System.out.println(template.getSqlStatement());
            System.out.println(args);
        }
        try {
            execute = Template.execute(res, template, args, ((PoolJdbc) pool).getStatementControl());
            pool.complete(res, null);
        } catch (Exception e) {
            e.printStackTrace();
            pool.complete(res, e);
        }
        return execute;
    }

    public List<Map<String, Object>> exec(String namePool, TemplateEnum templateEnum, Map<String, Object> args) throws Exception {
        return exec(namePool, templateEnum, args, false);
    }

    @SuppressWarnings("unused")
    public long addIfNotExist(String namePool, TemplateEnum selectSql, TemplateEnum insertSql, String idField, Map<String, Object> args) throws Exception {
        List<Map<String, Object>> select = exec(namePool, selectSql, args);
        Long id;
        if (!select.isEmpty()) {
            id = (Long) select.get(0).get(idField);
        } else {
            List<Map<String, Object>> insertWord = exec(namePool, insertSql, args);
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
