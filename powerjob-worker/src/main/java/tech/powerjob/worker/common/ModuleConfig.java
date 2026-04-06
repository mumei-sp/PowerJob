package tech.powerjob.worker.common;

import lombok.Getter;
import lombok.Setter;
import tech.powerjob.worker.extension.processor.ProcessorFactory;

import java.util.List;

/**
 * Configuration for a single module in multi-app-group mode.
 * Each module maps to one PowerJob app group and gets its own worker instance.
 *
 * @since 5.2.0
 */
@Getter
@Setter
public class ModuleConfig {

    /**
     * Application name (must match registered app on server).
     */
    private String appName;

    /**
     * Server addresses for this module's app group. Comma-separated for HA.
     */
    private List<String> serverAddress;

    /**
     * Worker port for this module. Use negative value for random port assignment.
     * Each module MUST use a different port.
     */
    private int port = -1;

    /**
     * Max concurrent lightweight tasks for this module.
     */
    private int maxLightweightTaskNum = 1024;

    /**
     * Max concurrent heavyweight tasks for this module.
     */
    private int maxHeavyweightTaskNum = 64;

    /**
     * Heartbeat interval in seconds.
     */
    private int healthReportInterval = 10;

    /**
     * Worker tag for dispatch filtering.
     */
    private String tag;

    /**
     * Custom processor factories for this module.
     */
    private List<ProcessorFactory> processorFactoryList;
}
