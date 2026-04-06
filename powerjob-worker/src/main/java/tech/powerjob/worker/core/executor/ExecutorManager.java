package tech.powerjob.worker.core.executor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.utils.SysUtils;
import tech.powerjob.worker.common.PowerJobWorkerConfig;

import java.util.concurrent.*;

/**
 * @author Echo009
 * @since 2022/9/23
 */
@Slf4j
@Getter
public class ExecutorManager {
    /**
     * 执行 Worker 底层核心任务
     */
    private final ScheduledExecutorService coreExecutor;
    /**
     * 执行轻量级任务状态上报
     */
    private final ScheduledExecutorService lightweightTaskStatusCheckExecutor;
    /**
     * 执行轻量级任务
     */
    private final ExecutorService lightweightTaskExecutorService;


    public ExecutorManager(PowerJobWorkerConfig workerConfig){

        // Use appName in thread names for multi-worker JVM observability (thread dumps, monitoring)
        String prefix = "powerjob-" + workerConfig.getAppName();

        final int availableProcessors = SysUtils.availableProcessors();
        // Defensive null handling for boxed Integer fields (could be null if explicitly set via setter)
        final int maxLightTasks = workerConfig.getMaxLightweightTaskNum() != null ? workerConfig.getMaxLightweightTaskNum() : 1024;
        // Cap IO pool size at maxLightweightTaskNum to avoid wasting threads for low-concurrency modules
        final int ioPoolSize = Math.max(1, Math.min(availableProcessors * 10, maxLightTasks));

        // 初始化定时线程池
        ThreadFactory coreThreadFactory = new ThreadFactoryBuilder().setNameFormat(prefix + "-core-%d").build();
        coreExecutor =  new ScheduledThreadPoolExecutor(3, coreThreadFactory);

        ThreadFactory lightTaskReportFactory = new ThreadFactoryBuilder().setNameFormat(prefix + "-light-status-%d").build();
        // 都是 io 密集型任务
        lightweightTaskStatusCheckExecutor =  new ScheduledThreadPoolExecutor(ioPoolSize, lightTaskReportFactory);

        ThreadFactory lightTaskExecuteFactory = new ThreadFactoryBuilder().setNameFormat(prefix + "-light-exec-%d").build();
        // 大部分任务都是 io 密集型
        lightweightTaskExecutorService = new ThreadPoolExecutor(ioPoolSize, ioPoolSize, 120L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>((maxLightTasks * 2),true), lightTaskExecuteFactory, new ThreadPoolExecutor.AbortPolicy());

        log.info("[ExecutorManager] {} initialized: ioPoolSize={} (cores={}, maxTasks={}, formula=min(cores*10,maxTasks))",
                prefix, ioPoolSize, availableProcessors, workerConfig.getMaxLightweightTaskNum());
    }



    public void shutdown(){
        coreExecutor.shutdownNow();
        lightweightTaskStatusCheckExecutor.shutdownNow();
        lightweightTaskExecutorService.shutdownNow();
    }

}
