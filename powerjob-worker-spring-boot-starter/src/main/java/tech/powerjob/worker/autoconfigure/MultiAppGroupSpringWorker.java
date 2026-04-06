package tech.powerjob.worker.autoconfigure;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import tech.powerjob.worker.MultiAppGroupWorker;
import tech.powerjob.worker.common.ModuleConfig;
import tech.powerjob.worker.common.MultiAppGroupWorkerConfig;
import tech.powerjob.worker.extension.processor.ProcessorFactory;
import tech.powerjob.worker.processor.impl.BuildInSpringMethodProcessorFactory;
import tech.powerjob.worker.processor.impl.BuiltInSpringProcessorFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Spring lifecycle wrapper for MultiAppGroupWorker.
 * Injects Spring-managed processor factories into each module's config
 * so that @Component-annotated processors are discoverable.
 *
 * @since 5.2.0
 */
@Slf4j
public class MultiAppGroupSpringWorker implements ApplicationContextAware, InitializingBean, DisposableBean {

    private MultiAppGroupWorker multiAppGroupWorker;
    private final MultiAppGroupWorkerConfig config;

    public MultiAppGroupSpringWorker(MultiAppGroupWorkerConfig config) {
        this.config = config;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        // BuiltInSpringProcessorFactory is stateless (bean lookup by name) — safe to share across modules
        BuiltInSpringProcessorFactory springProcessorFactory = new BuiltInSpringProcessorFactory(applicationContext);

        for (ModuleConfig moduleConfig : config.getModules().values()) {
            // BuildInSpringMethodProcessorFactory has per-instance jobHandlerRepository
            // — each module MUST get its own instance to avoid handler name collision
            BuildInSpringMethodProcessorFactory perModuleMethodFactory =
                    new BuildInSpringMethodProcessorFactory(applicationContext);

            List<ProcessorFactory> factories = Lists.newArrayList(
                    Optional.ofNullable(moduleConfig.getProcessorFactoryList())
                            .orElse(Collections.emptyList()));
            factories.add(springProcessorFactory);
            factories.add(perModuleMethodFactory);
            moduleConfig.setProcessorFactoryList(factories);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        multiAppGroupWorker = new MultiAppGroupWorker(config);
        multiAppGroupWorker.init();
    }

    @Override
    public void destroy() throws Exception {
        multiAppGroupWorker.destroy();
    }

    /**
     * Access the underlying coordinator for health checks or programmatic access.
     */
    public MultiAppGroupWorker getMultiAppGroupWorker() {
        return multiAppGroupWorker;
    }
}
