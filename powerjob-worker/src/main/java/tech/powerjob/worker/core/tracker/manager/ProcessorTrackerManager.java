package tech.powerjob.worker.core.tracker.manager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import tech.powerjob.worker.core.tracker.processor.ProcessorTracker;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 持有 Processor 对象
 * instanceId -> Processor
 *
 * @author tjq
 * @since 2020/3/20
 */
public class ProcessorTrackerManager {

    /**
     * instanceId -> (TaskTrackerAddress -> ProcessorTracker)
     * 处理脑裂情况下同一个 Instance 存在多个 TaskTracker 的情况
     */
    private final Map<Long, Map<String, ProcessorTracker>> processorTrackerContainer = Maps.newHashMap();

    /**
     * 获取 ProcessorTracker，如果不存在则创建
     */
    public synchronized ProcessorTracker getProcessorTracker(Long instanceId, String address, Supplier<ProcessorTracker> creator) {

        ProcessorTracker processorTracker = processorTrackerContainer.getOrDefault(instanceId, Collections.emptyMap()).get(address);
        if (processorTracker == null) {
            processorTracker = creator.get();
            processorTrackerContainer.computeIfAbsent(instanceId, ignore -> Maps.newHashMap()).put(address, processorTracker);
        }
        return processorTracker;
    }

    public synchronized List<ProcessorTracker> removeProcessorTracker(Long instanceId) {

        List<ProcessorTracker> res = Lists.newLinkedList();
        Map<String, ProcessorTracker> ttAddress2Pt = processorTrackerContainer.remove(instanceId);
        if (ttAddress2Pt != null) {
            res.addAll(ttAddress2Pt.values());
            ttAddress2Pt.clear();
        }
        return res;
    }
}
