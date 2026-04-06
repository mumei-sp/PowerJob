package tech.powerjob.worker.background.heartbeat;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import tech.powerjob.common.enhance.SafeRunnable;
import tech.powerjob.common.model.SystemMetrics;
import tech.powerjob.common.request.WorkerHeartbeat;
import tech.powerjob.worker.common.PowerJobWorkerVersion;
import tech.powerjob.worker.common.WorkerRuntime;
import tech.powerjob.worker.common.utils.TransportUtils;


/**
 * Worker健康度定时上报
 *
 * @author tjq
 * @since 2020/3/25
 */
@Slf4j
@RequiredArgsConstructor
public class WorkerHealthReporter extends SafeRunnable {

    private final WorkerRuntime workerRuntime;

    @Override
    public void run0() {

        // 没有可用Server，无法上报
        String currentServer = workerRuntime.getServerDiscoveryService().getCurrentServerAddress();
        if (StringUtils.isEmpty(currentServer)) {
            log.warn("[WorkerHealthReporter] no available server,fail to report health info!");
            return;
        }

        SystemMetrics systemMetrics;

        systemMetrics = workerRuntime.getWorkerConfig().getSystemMetricsCollector().collect();

        WorkerHeartbeat heartbeat = new WorkerHeartbeat();

        heartbeat.setSystemMetrics(systemMetrics);
        heartbeat.setWorkerAddress(workerRuntime.getWorkerAddress());
        heartbeat.setAppName(workerRuntime.getWorkerConfig().getAppName());
        heartbeat.setAppId(workerRuntime.getAppId());
        heartbeat.setHeartbeatTime(System.currentTimeMillis());
        heartbeat.setVersion(PowerJobWorkerVersion.getVersion());
        heartbeat.setProtocol(workerRuntime.getWorkerConfig().getProtocol().name());
        heartbeat.setClient("KingPenguin");
        heartbeat.setTag(workerRuntime.getWorkerConfig().getTag());

        // 上报 Tracker 数量
        int lightSize = workerRuntime.getLightTaskTrackerManager().currentTaskTrackerSize();
        int heavySize = workerRuntime.getHeavyTaskTrackerManager().currentTaskTrackerSize();
        heartbeat.setLightTaskTrackerNum(lightSize);
        heartbeat.setHeavyTaskTrackerNum(heavySize);
        // 是否超载
        if (workerRuntime.getWorkerConfig().getMaxLightweightTaskNum() <= lightSize || workerRuntime.getWorkerConfig().getMaxHeavyweightTaskNum() <= heavySize){
            heartbeat.setOverload(true);
        }
        // 获取当前加载的容器列表
        heartbeat.setContainerInfos(workerRuntime.getOmsContainerFactory().getDeployedContainerInfos());
        // 发送请求
        if (StringUtils.isEmpty(currentServer)) {
            return;
        }
        // log
        log.info("[WorkerHealthReporter] report health status,appId:{},appName:{},isOverload:{},maxLightweightTaskNum:{},currentLightweightTaskNum:{},maxHeavyweightTaskNum:{},currentHeavyweightTaskNum:{}" ,
                heartbeat.getAppId(),
                heartbeat.getAppName(),
                heartbeat.isOverload(),
                workerRuntime.getWorkerConfig().getMaxLightweightTaskNum(),
                heartbeat.getLightTaskTrackerNum(),
                workerRuntime.getWorkerConfig().getMaxHeavyweightTaskNum(),
                heartbeat.getHeavyTaskTrackerNum()
        );

        TransportUtils.reportWorkerHeartbeat(heartbeat, currentServer, workerRuntime.getTransporter());
    }
}
