package tech.powerjob.worker.core.tracker.manager;

import com.google.common.collect.Maps;
import tech.powerjob.worker.core.tracker.task.light.LightTaskTracker;

import java.util.Map;
import java.util.function.Function;

/**
 * @author Echo009
 * @since 2022/9/23
 */
public class LightTaskTrackerManager {

    public static final double OVERLOAD_FACTOR = 1.3d;

    private final Map<Long, LightTaskTracker> instanceId2TaskTracker = Maps.newConcurrentMap();


    public LightTaskTracker getTaskTracker(Long instanceId) {
        return instanceId2TaskTracker.get(instanceId);
    }

    public void removeTaskTracker(Long instanceId) {
        // This containsKey check is critical to prevent deadlock when destroy() is called during computeIfAbsent error paths
        if (instanceId2TaskTracker.containsKey(instanceId)) {
            instanceId2TaskTracker.remove(instanceId);
        }
    }

    public void atomicCreateTaskTracker(Long instanceId, Function<Long, LightTaskTracker> creator) {
        instanceId2TaskTracker.computeIfAbsent(instanceId, creator);
    }

    public int currentTaskTrackerSize(){
        return instanceId2TaskTracker.size();
    }

}
