package tech.powerjob.worker.background.heartbeat;

import tech.powerjob.common.model.SystemMetrics;
import tech.powerjob.worker.extension.SystemMetricsCollector;

/**
 * A caching wrapper around SystemMetricsCollector that avoids redundant disk I/O
 * when multiple workers share the same JVM. The first call in each TTL window
 * performs real collection; subsequent calls return the cached result.
 *
 * <p>Thread-safe via volatile + double-checked locking.
 *
 * @since 5.2.0
 */
public class CachingSystemMetricsCollector implements SystemMetricsCollector {

    private final SystemMetricsCollector delegate;
    private final long cacheTtlMs;
    private volatile SystemMetrics cached;
    private volatile long lastCollectTime;

    /**
     * @param delegate the real collector to wrap
     * @param cacheTtlMs cache time-to-live in milliseconds (e.g. 5000 for 5 seconds)
     */
    public CachingSystemMetricsCollector(SystemMetricsCollector delegate, long cacheTtlMs) {
        this.delegate = delegate;
        this.cacheTtlMs = cacheTtlMs;
    }

    @Override
    public SystemMetrics collect() {
        // Snapshot volatile fields into locals for consistent fast-path check
        SystemMetrics localCached = cached;
        long localLastCollectTime = lastCollectTime;
        long now = System.currentTimeMillis();

        // Fast path: cache is fresh (both reads are from a single consistent snapshot)
        if (localCached != null && (now - localLastCollectTime) < cacheTtlMs) {
            return localCached;
        }
        // Slow path: refresh cache (only one thread does the actual collection)
        synchronized (this) {
            // Re-check with fresh volatile reads inside lock (another thread may have refreshed)
            if (cached != null && (System.currentTimeMillis() - lastCollectTime) < cacheTtlMs) {
                return cached;
            }
            cached = delegate.collect();
            lastCollectTime = System.currentTimeMillis();
            return cached;
        }
    }
}
