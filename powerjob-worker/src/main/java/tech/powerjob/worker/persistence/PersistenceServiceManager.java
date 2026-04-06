package tech.powerjob.worker.persistence;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * 持久化器管理
 *
 * @author tjq
 * @since 2024/2/25
 */
public class PersistenceServiceManager {

    private final Map<Long, TaskPersistenceService> instanceId2TaskPersistenceService = Maps.newConcurrentMap();

    public void register(Long instanceId, TaskPersistenceService taskPersistenceService) {
        instanceId2TaskPersistenceService.put(instanceId, taskPersistenceService);
    }

    public void unregister(Long instanceId) {
        instanceId2TaskPersistenceService.remove(instanceId);
    }

    public TaskPersistenceService fetchTaskPersistenceService(Long instanceId) {
        return instanceId2TaskPersistenceService.get(instanceId);
    }
}
