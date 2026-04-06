package tech.powerjob.worker.core.tracker.manager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import tech.powerjob.worker.core.tracker.task.heavy.FrequentTaskTracker;
import tech.powerjob.worker.core.tracker.task.heavy.HeavyTaskTracker;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 持有 TaskTracker 对象
 *
 * @author tjq
 * @since 2020/3/24
 */
public class HeavyTaskTrackerManager {

    private final Map<Long, HeavyTaskTracker> instanceId2TaskTracker = Maps.newConcurrentMap();
    /**
     * 获取 TaskTracker
     */
    public HeavyTaskTracker getTaskTracker(Long instanceId) {
        return instanceId2TaskTracker.get(instanceId);
    }

    public HeavyTaskTracker removeTaskTracker(Long instanceId) {
        return instanceId2TaskTracker.remove(instanceId);
    }

    public void atomicCreateTaskTracker(Long instanceId, Function<Long, HeavyTaskTracker> creator) {
        instanceId2TaskTracker.computeIfAbsent(instanceId, creator);
    }

    public List<Long> getAllFrequentTaskTrackerKeys() {
        List<Long> keys = Lists.newLinkedList();
        instanceId2TaskTracker.forEach((key, tk) -> {
            if (tk instanceof FrequentTaskTracker) {
                keys.add(key);
            }
        });
        return keys;
    }

    public int currentTaskTrackerSize(){
        return instanceId2TaskTracker.size();
    }
}
