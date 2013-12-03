package org.gbif.occurrencestore.processor.zookeeper;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.inject.Singleton;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.atomic.DistributedAtomicLong;
import com.netflix.curator.retry.RetryNTimes;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A connector to Zookeeper that provides convenience methods for incrementing and reading counts.
 */
@Singleton
public class ZookeeperConnector {

  private static final String CRAWL_PREFIX = "/crawls/";
  private static final int BATCH_FLUSH_INTERVAL = 1000;

  private final CuratorFramework curator;

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperConnector.class);

  private final Timer addTimer =
    Metrics.newTimer(ZookeeperConnector.class, "counter add time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  private final Timer incrementTimer =
    Metrics.newTimer(ZookeeperConnector.class, "dal inc time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  private final BatchingDalWrapper dalWrapper;
  private static final Set<String> ENSURED_PATHS = Collections.synchronizedSet(new HashSet<String>());


  /**
   * Builds the connector with the given curator. Note that if the curator is not started before being passed in it
   * will be started here.
   *
   * @param curator the CuratorFramework providing underlying connections to zookeeper
   */
  public ZookeeperConnector(final CuratorFramework curator) {
    checkNotNull(curator, "curator can't be null");
    if (!curator.isStarted()) {
      curator.start();
    }
    this.curator = curator;
    dalWrapper = new BatchingDalWrapper(curator, BATCH_FLUSH_INTERVAL);
  }

  /**
   * Atomically adds to the count for this dataset and counterName.
   *
   * @param datasetUuid the dataset key
   * @param counterName the counterName representing a processing phase
   */
  public void addCounter(UUID datasetUuid, CounterName counterName) {
    checkNotNull(datasetUuid);
    checkNotNull(counterName);
    final TimerContext context = addTimer.time();
    try {
      try {
        String path = CRAWL_PREFIX + datasetUuid.toString() + counterName.getPath();
        LOG.debug("Updating DAL at path [{}]", path);
        if (!ENSURED_PATHS.contains(path)) {
          curator.newNamespaceAwareEnsurePath(path).ensure(curator.getZookeeperClient());
          ENSURED_PATHS.add(path);
        }
        final TimerContext incContext = incrementTimer.time();
        try {
          dalWrapper.increment(path);
        } finally {
          incContext.stop();
        }
      } catch (Exception e) {
        LOG.warn("Error updating zookeeper", e);
      }
    } finally {
      context.stop();
    }
  }

  /**
   * Read the count for this dataset and counterName.
   *
   * @param datasetUuid the dataset key
   * @param counterName the counterName representing a processing phase
   *
   * @return the count or null if it could not be read
   */
  @Nullable
  public Long readCounter(UUID datasetUuid, CounterName counterName) {
    checkNotNull(datasetUuid);
    checkNotNull(counterName);
    Long result = null;
    try {
      String path = CRAWL_PREFIX + datasetUuid.toString() + counterName.getPath();
      LOG.debug("Reading DAL at path [{}]", path);
      curator.newNamespaceAwareEnsurePath(path).ensure(curator.getZookeeperClient());
      DistributedAtomicLong dal = new DistributedAtomicLong(curator, path, new RetryNTimes(1, 1000));
      result = dal.get().preValue();
    } catch (Exception e) {
      LOG.warn("Error reading from zookeeper", e);
    }
    return result;
  }

  public enum CounterName {
    FRAGMENT_RECEIVED("/fragmentsReceived"),
    RAW_OCCURRENCE_PERSISTED_NEW("/rawOccurrencesPersisted/new"),
    RAW_OCCURRENCE_PERSISTED_UPDATED("/rawOccurrencesPersisted/updated"),
    RAW_OCCURRENCE_PERSISTED_UNCHANGED("/rawOccurrencesPersisted/unchanged"),
    RAW_OCCURRENCE_PERSISTED_ERROR("/rawOccurrencesPersisted/error"),
    FRAGMENT_PROCESSED("/fragmentsProcessed"),
    VERBATIM_OCCURRENCE_PERSISTED_SUCCESS("/verbatimOccurrencesPersisted/successful"),
    VERBATIM_OCCURRENCE_PERSISTED_ERROR("/verbatimOccurrencesPersisted/error"),
    INTERPRETED_OCCURRENCE_PERSISTED_SUCCESS("/interpretedOccurrencesPersisted/successful"),
    INTERPRETED_OCCURRENCE_PERSISTED_ERROR("/interpretedOccurrencesPersisted/error");

    private final String path;

    private CounterName(String path) {
      this.path = path;
    }

    public static CounterName fromPath(String path) {
      for (CounterName counterName : CounterName.values()) {
        if (counterName.getPath().equals(path)) {
          return counterName;
        }
      }
      throw new IllegalArgumentException("No CounterName for path [" + path + "]");
    }

    public String getPath() {
      return this.path;
    }
  }
}
