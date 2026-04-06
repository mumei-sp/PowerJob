package tech.powerjob.worker.persistence;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.worker.common.constants.StoreStrategy;
import tech.powerjob.worker.common.constants.TaskStatus;
import tech.powerjob.worker.core.processor.TaskResult;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Lazy proxy for TaskPersistenceService that defers H2 database initialization
 * until first actual use. For standalone-only workloads (LightTaskTracker),
 * the H2 database is never touched, saving HikariCP connections and disk I/O.
 *
 * <p>Thread-safe via volatile + double-checked locking.
 *
 * @since 5.2.0
 */
@Slf4j
public class LazyTaskPersistenceService implements TaskPersistenceService {

    private final StoreStrategy storeStrategy;
    private volatile TaskPersistenceService delegate;

    public LazyTaskPersistenceService(StoreStrategy storeStrategy) {
        this.storeStrategy = storeStrategy;
    }

    private TaskPersistenceService getOrInit() {
        if (delegate != null) {
            return delegate;
        }
        synchronized (this) {
            if (delegate != null) {
                return delegate;
            }
            log.info("[LazyTaskPersistenceService] first use detected, initializing H2 database (strategy={})", storeStrategy);
            try {
                DbTaskPersistenceService real = new DbTaskPersistenceService(storeStrategy);
                real.init();
                delegate = real;
            } catch (Exception e) {
                throw new RuntimeException("Failed to lazily initialize H2 TaskPersistenceService", e);
            }
            return delegate;
        }
    }

    @Override
    public void init() {
        // No-op: initialization is deferred to first actual use
    }

    @Override
    public boolean batchSave(List<TaskDO> tasks) {
        return getOrInit().batchSave(tasks);
    }

    @Override
    public boolean updateTask(Long instanceId, String taskId, TaskDO updateEntity) {
        return getOrInit().updateTask(instanceId, taskId, updateEntity);
    }

    @Override
    public boolean updateTaskStatus(Long instanceId, String taskId, int status, long lastReportTime, String result) {
        return getOrInit().updateTaskStatus(instanceId, taskId, status, lastReportTime, result);
    }

    @Override
    public boolean updateLostTasks(Long instanceId, List<String> addressList, boolean retry) {
        return getOrInit().updateLostTasks(instanceId, addressList, retry);
    }

    @Override
    public Optional<TaskDO> getLastTask(Long instanceId, Long subInstanceId) {
        return getOrInit().getLastTask(instanceId, subInstanceId);
    }

    @Override
    public List<TaskDO> getAllUnFinishedTaskByAddress(Long instanceId, String address) {
        return getOrInit().getAllUnFinishedTaskByAddress(instanceId, address);
    }

    @Override
    public List<TaskDO> getTaskByStatus(Long instanceId, TaskStatus status, int limit) {
        return getOrInit().getTaskByStatus(instanceId, status, limit);
    }

    @Override
    public List<TaskDO> getTaskByQuery(Long instanceId, String customQuery) {
        return getOrInit().getTaskByQuery(instanceId, customQuery);
    }

    @Override
    public Map<TaskStatus, Long> getTaskStatusStatistics(Long instanceId, Long subInstanceId) {
        return getOrInit().getTaskStatusStatistics(instanceId, subInstanceId);
    }

    @Override
    public List<TaskResult> getAllTaskResult(Long instanceId, Long subInstanceId) {
        return getOrInit().getAllTaskResult(instanceId, subInstanceId);
    }

    @Override
    public Optional<TaskDO> getTask(Long instanceId, String taskId) {
        return getOrInit().getTask(instanceId, taskId);
    }

    @Override
    public boolean deleteAllTasks(Long instanceId) {
        return getOrInit().deleteAllTasks(instanceId);
    }

    @Override
    public boolean deleteAllSubInstanceTasks(Long instanceId, Long subInstanceId) {
        return getOrInit().deleteAllSubInstanceTasks(instanceId, subInstanceId);
    }

    @Override
    public boolean deleteTasksByTaskIds(Long instanceId, Collection<String> taskId) {
        return getOrInit().deleteTasksByTaskIds(instanceId, taskId);
    }
}
