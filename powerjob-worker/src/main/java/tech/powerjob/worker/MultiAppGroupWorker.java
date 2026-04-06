package tech.powerjob.worker;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.utils.CommonUtils;
import tech.powerjob.common.enums.Protocol;
import tech.powerjob.worker.background.heartbeat.CachingSystemMetricsCollector;
import tech.powerjob.worker.background.heartbeat.DefaultSystemMetricsCollector;
import tech.powerjob.worker.common.ModuleConfig;
import tech.powerjob.worker.common.MultiAppGroupWorkerConfig;
import tech.powerjob.worker.common.PowerJobWorkerConfig;
import tech.powerjob.remote.http.vertx.VertxInitializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Coordinator for running multiple PowerJobWorker instances in a single JVM.
 * Each module gets its own independent worker with its own port, app group,
 * server discovery, heartbeat, thread pools, and task tracking.
 *
 * <p>Usage:
 * <pre>
 * MultiAppGroupWorkerConfig config = new MultiAppGroupWorkerConfig();
 * config.setProtocol(Protocol.HTTP);
 *
 * ModuleConfig payments = new ModuleConfig();
 * payments.setAppName("payments-app");
 * payments.setServerAddress(Arrays.asList("10.0.0.1:7700"));
 * payments.setMaxLightweightTaskNum(50);
 *
 * ModuleConfig reports = new ModuleConfig();
 * reports.setAppName("reports-app");
 * reports.setServerAddress(Arrays.asList("10.0.0.1:7700"));
 * reports.setMaxLightweightTaskNum(100);
 *
 * config.getModules().put("payments", payments);
 * config.getModules().put("reports", reports);
 *
 * MultiAppGroupWorker worker = new MultiAppGroupWorker(config);
 * worker.init();
 * </pre>
 *
 * @since 5.2.0
 */
@Slf4j
public class MultiAppGroupWorker {

    private final MultiAppGroupWorkerConfig config;
    private final Map<String, PowerJobWorker> moduleWorkers = new LinkedHashMap<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private volatile Object sharedVertx;

    public MultiAppGroupWorker(MultiAppGroupWorkerConfig config) {
        CommonUtils.requireNonNull(config, "MultiAppGroupWorkerConfig can't be null");
        CommonUtils.requireNonNull(config.getModules(), "modules can't be null");
        if (config.getModules().isEmpty()) {
            throw new IllegalArgumentException("At least one module must be configured");
        }
        validateConfig(config);
        this.config = config;
    }

    /**
     * Initialize all module workers. Each module gets its own PowerJobWorker instance
     * bound to its own port, registering with its own app group.
     */
    public void init() throws Exception {

        if (!initialized.compareAndSet(false, true)) {
            log.warn("[MultiAppGroupWorker] already initialized, skipping");
            return;
        }

        log.info("[MultiAppGroupWorker] starting {} module workers: {}", config.getModules().size(), config.getModules().keySet());

        // Auto-install caching metrics collector to avoid redundant disk I/O across workers
        if (config.getSystemMetricsCollector() == null) {
            config.setSystemMetricsCollector(
                    new CachingSystemMetricsCollector(new DefaultSystemMetricsCollector(), 5000));
            log.info("[MultiAppGroupWorker] installed CachingSystemMetricsCollector (5s TTL) for shared metrics collection");
        }

        // Create shared Vert.x instance for HTTP protocol to avoid N×56 thread duplication
        if (config.getProtocol() == Protocol.HTTP && config.getModules().size() > 1) {
            this.sharedVertx = VertxInitializer.buildVertx();
            log.info("[MultiAppGroupWorker] created shared Vertx instance for {} modules", config.getModules().size());
        }

        List<String> failedModules = new ArrayList<>();

        for (Map.Entry<String, ModuleConfig> entry : config.getModules().entrySet()) {
            String moduleName = entry.getKey();
            ModuleConfig moduleConfig = entry.getValue();

            PowerJobWorkerConfig workerConfig = buildWorkerConfig(moduleName, moduleConfig);
            PowerJobWorker worker = new PowerJobWorker(workerConfig);

            try {
                worker.init();
                moduleWorkers.put(moduleName, worker);
                log.info("[MultiAppGroupWorker] module '{}' (appName={}) initialized successfully", moduleName, moduleConfig.getAppName());
            } catch (Exception e) {
                log.error("[MultiAppGroupWorker] failed to initialize module '{}' (appName={})", moduleName, moduleConfig.getAppName(), e);

                if (!config.isAllowPartialStart()) {
                    destroyQuietly();
                    throw e;
                }
                failedModules.add(moduleName);
            }
        }

        if (moduleWorkers.isEmpty()) {
            initialized.set(false);
            throw new IllegalStateException("[MultiAppGroupWorker] all modules failed to initialize: " + failedModules);
        }

        if (!failedModules.isEmpty()) {
            log.warn("[MultiAppGroupWorker] partial start: {} modules failed: {}, {} modules running: {}",
                    failedModules.size(), failedModules, moduleWorkers.size(), moduleWorkers.keySet());
        }

        log.info("[MultiAppGroupWorker] {} module workers initialized successfully", moduleWorkers.size());
    }

    /**
     * Destroy all module workers gracefully.
     */
    public void destroy() throws Exception {
        log.info("[MultiAppGroupWorker] shutting down {} module workers", moduleWorkers.size());
        Exception firstException = null;

        for (Map.Entry<String, PowerJobWorker> entry : moduleWorkers.entrySet()) {
            try {
                entry.getValue().destroy();
                log.info("[MultiAppGroupWorker] module '{}' destroyed successfully", entry.getKey());
            } catch (Exception e) {
                log.error("[MultiAppGroupWorker] failed to destroy module '{}'", entry.getKey(), e);
                if (firstException == null) {
                    firstException = e;
                }
            }
        }
        moduleWorkers.clear();

        // Close shared Vertx AFTER all workers are destroyed (their HttpServers/Clients are already closed)
        closeSharedVertx();

        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * Get a specific module's worker instance.
     * @return the worker, or null if the module doesn't exist or failed to start
     */
    public PowerJobWorker getWorker(String moduleName) {
        return moduleWorkers.get(moduleName);
    }

    /**
     * Get all running module workers (unmodifiable view).
     */
    public Map<String, PowerJobWorker> getAllWorkers() {
        return Collections.unmodifiableMap(moduleWorkers);
    }

    /**
     * Returns health status for each running module.
     */
    public Map<String, ModuleHealthStatus> getHealthStatus() {
        Map<String, ModuleHealthStatus> status = new LinkedHashMap<>();
        for (Map.Entry<String, PowerJobWorker> entry : moduleWorkers.entrySet()) {
            String moduleName = entry.getKey();
            PowerJobWorker worker = entry.getValue();
            ModuleHealthStatus mhs = new ModuleHealthStatus();
            mhs.moduleName = moduleName;
            mhs.appName = worker.workerRuntime.getWorkerConfig().getAppName();
            mhs.workerAddress = worker.workerRuntime.getWorkerAddress();
            mhs.connected = worker.workerRuntime.getServerDiscoveryService().getCurrentServerAddress() != null;
            mhs.lightTaskCount = worker.workerRuntime.getLightTaskTrackerManager().currentTaskTrackerSize();
            mhs.heavyTaskCount = worker.workerRuntime.getHeavyTaskTrackerManager().currentTaskTrackerSize();
            mhs.maxLightweightTaskNum = worker.workerRuntime.getWorkerConfig().getMaxLightweightTaskNum();
            mhs.maxHeavyweightTaskNum = worker.workerRuntime.getWorkerConfig().getMaxHeavyweightTaskNum();
            status.put(moduleName, mhs);
        }
        return status;
    }

    /**
     * Health status snapshot for a single module.
     */
    public static class ModuleHealthStatus {
        public String moduleName;
        public String appName;
        public String workerAddress;
        public boolean connected;
        public int lightTaskCount;
        public int heavyTaskCount;
        public int maxLightweightTaskNum;
        public int maxHeavyweightTaskNum;

        public boolean isOverloaded() {
            return lightTaskCount >= maxLightweightTaskNum || heavyTaskCount >= maxHeavyweightTaskNum;
        }

        @Override
        public String toString() {
            return String.format("Module[%s/%s@%s connected=%s light=%d/%d heavy=%d/%d overloaded=%s]",
                    moduleName, appName, workerAddress, connected,
                    lightTaskCount, maxLightweightTaskNum,
                    heavyTaskCount, maxHeavyweightTaskNum,
                    isOverloaded());
        }
    }

    private void validateConfig(MultiAppGroupWorkerConfig config) {
        Map<String, ModuleConfig> modules = config.getModules();

        // Validate each module has required fields
        for (Map.Entry<String, ModuleConfig> entry : modules.entrySet()) {
            String name = entry.getKey();
            ModuleConfig mc = entry.getValue();
            CommonUtils.requireNonNull(mc.getAppName(), "appName is required for module: " + name);
            CommonUtils.requireNonNull(mc.getServerAddress(), "serverAddress is required for module: " + name);
            if (mc.getServerAddress().isEmpty()) {
                throw new IllegalArgumentException("serverAddress must not be empty for module: " + name);
            }
        }

        // Detect duplicate appNames
        Map<String, List<String>> appNameToModules = modules.entrySet().stream()
                .collect(Collectors.groupingBy(
                        e -> e.getValue().getAppName(),
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
        appNameToModules.forEach((appName, moduleNames) -> {
            if (moduleNames.size() > 1) {
                throw new IllegalArgumentException(
                        "Duplicate appName '" + appName + "' found in modules: " + moduleNames
                        + ". Each module must have a unique appName.");
            }
        });

        // Detect duplicate fixed ports (ignoring random port assignments where port <= 0)
        Map<Integer, List<String>> portToModules = modules.entrySet().stream()
                .filter(e -> e.getValue().getPort() > 0)
                .collect(Collectors.groupingBy(
                        e -> e.getValue().getPort(),
                        Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
        portToModules.forEach((port, moduleNames) -> {
            if (moduleNames.size() > 1) {
                throw new IllegalArgumentException(
                        "Duplicate port " + port + " found in modules: " + moduleNames
                        + ". Each module must use a different port, or use -1 for random assignment.");
            }
        });
    }

    private PowerJobWorkerConfig buildWorkerConfig(String moduleName, ModuleConfig mc) {
        PowerJobWorkerConfig wc = new PowerJobWorkerConfig();

        // Per-module settings
        wc.setAppName(mc.getAppName());
        wc.setServerAddress(Lists.newArrayList(mc.getServerAddress()));
        wc.setPort(mc.getPort());
        wc.setMaxLightweightTaskNum(mc.getMaxLightweightTaskNum());
        wc.setMaxHeavyweightTaskNum(mc.getMaxHeavyweightTaskNum());
        wc.setHealthReportInterval(mc.getHealthReportInterval());
        wc.setTag(mc.getTag());
        wc.setProcessorFactoryList(mc.getProcessorFactoryList());

        // Shared settings from MultiAppGroupWorkerConfig
        wc.setProtocol(config.getProtocol());
        wc.setStoreStrategy(config.getStoreStrategy());
        wc.setUserContext(config.getUserContext());
        wc.setMaxResultLength(config.getMaxResultLength());
        wc.setMaxAppendedWfContextLength(config.getMaxAppendedWfContextLength());
        wc.setAllowLazyConnectServer(config.isAllowLazyConnectServer());
        wc.setSystemMetricsCollector(config.getSystemMetricsCollector());
        wc.setSharedTransportEngine(sharedVertx);

        return wc;
    }

    private void destroyQuietly() {
        for (Map.Entry<String, PowerJobWorker> entry : moduleWorkers.entrySet()) {
            try {
                entry.getValue().destroy();
            } catch (Exception e) {
                log.warn("[MultiAppGroupWorker] error destroying module '{}' during rollback", entry.getKey(), e);
            }
        }
        moduleWorkers.clear();
        closeSharedVertx();
    }

    private void closeSharedVertx() {
        if (sharedVertx != null) {
            try {
                if (sharedVertx instanceof io.vertx.core.Vertx) {
                    ((io.vertx.core.Vertx) sharedVertx).close();
                    log.info("[MultiAppGroupWorker] shared Vertx instance closed");
                }
            } catch (Exception e) {
                log.warn("[MultiAppGroupWorker] error closing shared Vertx", e);
            }
            sharedVertx = null;
        }
    }
}
