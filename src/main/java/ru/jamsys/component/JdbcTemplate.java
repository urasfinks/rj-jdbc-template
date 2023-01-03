package ru.jamsys.component;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import ru.jamsys.AbstractCoreComponent;

import ru.jamsys.jdbc.template.Template;
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
    private final Map<String, Template> mapTemplate = new ConcurrentHashMap<>();

    public JdbcTemplate(StatisticAggregator statisticAggregator, Scheduler scheduler) {
        this.scheduler = scheduler;
        this.statisticAggregator = statisticAggregator;
        scheduler.add(nameSchedulerStabilizer, this::stabilizer, 1000);
        scheduler.add(SchedulerGlobal.SCHEDULER_GLOBAL_STATISTIC_WRITE, this::flushStatistic);
    }

    public void addPool(Pool<Connection> pool) {
        mapPool.put(pool.getName(), pool);
    }

    public Pool<Connection> getPool(String name) {
        return mapPool.get(name);
    }

    public void addTemplate(String name, Template template) {
        mapTemplate.put(name, template);
    }

    public Template getTemplate(String name) {
        return mapTemplate.get(name);
    }

    public List<Map<String, Object>> exec(String namePool, String nameTemplate, Map<String, Object> args) throws Exception {
        Pool<Connection> pool = getPool(namePool);
        Template template = getTemplate(nameTemplate);
        Connection res = pool.getResource();
        List<Map<String, Object>> execute = null;
        try {
            execute = Template.execute(res, template, args, ((PoolJdbc) pool).getStatementControl());
            pool.complete(res, null);
        } catch (Exception e) {
            pool.complete(res, e);
        }
        return execute;
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
