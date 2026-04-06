package tech.powerjob.worker.common;

import lombok.Getter;
import lombok.Setter;
import tech.powerjob.common.enums.Protocol;
import tech.powerjob.worker.common.constants.StoreStrategy;
import tech.powerjob.worker.extension.SystemMetricsCollector;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration for running multiple PowerJob workers in a single JVM.
 * Shared settings (protocol, storeStrategy, etc.) are defined once at this level.
 * Per-module settings (appName, serverAddress, thread pools) are in each ModuleConfig.
 *
 * @since 5.2.0
 */
@Getter
@Setter
public class MultiAppGroupWorkerConfig {

    /**
     * Protocol for communication between workers and servers. HTTP recommended for multi-worker mode.
     */
    private Protocol protocol = Protocol.HTTP;

    /**
     * Internal persistence strategy (DISK or MEMORY).
     */
    private StoreStrategy storeStrategy = StoreStrategy.DISK;

    /**
     * Shared system metrics collector. When set, all workers reuse this instance
     * to avoid redundant CPU/memory/disk metric collection.
     */
    private SystemMetricsCollector systemMetricsCollector;

    /**
     * User-defined context object, passed through to TaskContext#userContext in all modules.
     */
    private Object userContext;

    /**
     * Max length of response result. Shared across all modules.
     */
    private int maxResultLength = 8096;

    /**
     * Max length of appended workflow context. Shared across all modules.
     */
    private int maxAppendedWfContextLength = 8192;

    /**
     * Allow workers to start without server connection. Shared across all modules.
     */
    private boolean allowLazyConnectServer = false;

    /**
     * When true, modules that fail to initialize (e.g. server unreachable) are skipped
     * instead of aborting the entire startup. Failed modules are logged as errors.
     * Default false — fail-fast for safety.
     */
    private boolean allowPartialStart = false;

    /**
     * Per-module configurations. Key is the module name (used for logging/thread naming).
     * Each module creates an independent PowerJobWorker instance.
     */
    private Map<String, ModuleConfig> modules = new LinkedHashMap<>();
}
