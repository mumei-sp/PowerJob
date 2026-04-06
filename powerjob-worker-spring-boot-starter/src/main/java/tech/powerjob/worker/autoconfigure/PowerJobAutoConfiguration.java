package tech.powerjob.worker.autoconfigure;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tech.powerjob.common.enums.Protocol;
import tech.powerjob.common.utils.CommonUtils;
import tech.powerjob.common.utils.NetUtils;
import tech.powerjob.worker.PowerJobSpringWorker;
import tech.powerjob.worker.common.ModuleConfig;
import tech.powerjob.worker.common.MultiAppGroupWorkerConfig;
import tech.powerjob.worker.common.PowerJobWorkerConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Autoconfiguration class for PowerJob-worker.
 * <p>
 * Supports two modes:
 * <ul>
 *   <li><b>Single-app mode</b> (default): Uses {@code powerjob.worker.*} properties → creates one PowerJobSpringWorker</li>
 *   <li><b>Multi-app mode</b>: Uses {@code powerjob.modules.*} properties → creates MultiAppGroupSpringWorker with N workers</li>
 * </ul>
 *
 * @author songyinyin
 * @since 2020/7/26 16:37
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(PowerJobProperties.class)
@ConditionalOnProperty(prefix = "powerjob.worker", name = "enabled", havingValue = "true", matchIfMissing = true)
public class PowerJobAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean({PowerJobSpringWorker.class, MultiAppGroupSpringWorker.class})
    public Object initPowerJob(PowerJobProperties properties) {

        Map<String, PowerJobProperties.ModuleWorker> modules = properties.getModules();

        // Multi-app mode: powerjob.modules is configured
        if (modules != null && !modules.isEmpty()) {
            if (StringUtils.isNotEmpty(properties.getWorker().getAppName())) {
                log.warn("[PowerJobAutoConfiguration] both powerjob.worker.app-name and powerjob.modules are configured. "
                        + "Using multi-app mode (powerjob.modules). The single-worker powerjob.worker.app-name config is ignored.");
            }
            return initMultiAppGroupWorker(properties);
        }

        // Single-app mode: traditional powerjob.worker.* config
        return initSingleWorker(properties);
    }

    private PowerJobSpringWorker initSingleWorker(PowerJobProperties properties) {

        PowerJobProperties.Worker worker = properties.getWorker();

        CommonUtils.requireNonNull(worker.getServerAddress(), "serverAddress can't be empty! " +
            "if you don't want to enable powerjob, please config program arguments: powerjob.worker.enabled=false");
        List<String> serverAddress = Arrays.asList(worker.getServerAddress().split(","));

        PowerJobWorkerConfig config = new PowerJobWorkerConfig();

        if (worker.getPort() != null) {
            config.setPort(worker.getPort());
        } else {
            int port = worker.getAkkaPort();
            if (port <= 0) {
                port = NetUtils.getRandomPort();
            }
            config.setPort(port);
        }

        config.setAppName(worker.getAppName());
        config.setServerAddress(serverAddress);
        config.setProtocol(worker.getProtocol());
        config.setStoreStrategy(worker.getStoreStrategy());
        config.setAllowLazyConnectServer(worker.isAllowLazyConnectServer());
        config.setMaxAppendedWfContextLength(worker.getMaxAppendedWfContextLength());
        config.setTag(worker.getTag());
        config.setMaxHeavyweightTaskNum(worker.getMaxHeavyweightTaskNum());
        config.setMaxLightweightTaskNum(worker.getMaxLightweightTaskNum());
        config.setHealthReportInterval(worker.getHealthReportInterval());

        return new PowerJobSpringWorker(config);
    }

    private MultiAppGroupSpringWorker initMultiAppGroupWorker(PowerJobProperties properties) {

        PowerJobProperties.Worker sharedWorker = properties.getWorker();

        MultiAppGroupWorkerConfig config = new MultiAppGroupWorkerConfig();
        config.setProtocol(sharedWorker.getProtocol());
        config.setStoreStrategy(sharedWorker.getStoreStrategy());
        config.setMaxResultLength(sharedWorker.getMaxResultLength());
        config.setMaxAppendedWfContextLength(sharedWorker.getMaxAppendedWfContextLength());
        config.setAllowLazyConnectServer(sharedWorker.isAllowLazyConnectServer());

        for (Map.Entry<String, PowerJobProperties.ModuleWorker> entry : properties.getModules().entrySet()) {
            String moduleName = entry.getKey();
            PowerJobProperties.ModuleWorker mw = entry.getValue();

            ModuleConfig mc = new ModuleConfig();
            mc.setAppName(mw.getAppName());

            CommonUtils.requireNonNull(mw.getServerAddress(),
                    "serverAddress can't be empty for module: " + moduleName);
            mc.setServerAddress(Arrays.asList(mw.getServerAddress().split(",")));

            if (mw.getPort() != null) {
                mc.setPort(mw.getPort());
            }
            // else default -1 (random) from ModuleConfig

            mc.setMaxLightweightTaskNum(mw.getMaxLightweightTaskNum());
            mc.setMaxHeavyweightTaskNum(mw.getMaxHeavyweightTaskNum());
            mc.setHealthReportInterval(mw.getHealthReportInterval());
            mc.setTag(mw.getTag());

            config.getModules().put(moduleName, mc);
        }

        return new MultiAppGroupSpringWorker(config);
    }
}
