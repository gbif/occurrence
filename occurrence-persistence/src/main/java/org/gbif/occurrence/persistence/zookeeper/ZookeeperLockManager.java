package org.gbif.occurrence.persistence.zookeeper;

import org.gbif.api.exception.ServiceUnavailableException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages locks in zookeeper on dataset keys (uuids). Initial use is for the purposes of creating new occurrence keys.
 * Note that this class is not thread-safe: each thread should create its own instance of this class (although they
 * can (and should) all share the same CuratorFramework instance).
 */
public class ZookeeperLockManager {

  private final CuratorFramework curator;
  private final Map<String, InterProcessSemaphoreMutex> locks = Maps.newHashMap();

  private static final String PATH_TO_LOCKS = "/datasetLocks/";
  private static final long MILLISECONDS_TO_WAIT = 0;

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperLockManager.class);

  /**
   * Create a new instance, one for every thread that wants lock access.
   *
   * @param curator a started curator that holds the zookeeper connection
   */
  @Inject
  public ZookeeperLockManager(CuratorFramework curator) {
    this.curator = checkNotNull(curator, "curator can't be null");
  }

  /**
   * Get a lock for the passed in dataset. If the lock has already been taken, or there is any error in the process
   * of acquiring the lock, false will be returned.
   *
   * @param datasetKey the dataset on which to lock
   *
   * @return true if the lock was acquired
   */
  public boolean getLock(String datasetKey) {
    checkNotNull(datasetKey);

    boolean gotLock = false;
    String path = buildPath(datasetKey);

    InterProcessSemaphoreMutex lock = locks.get(path);
    if (lock == null) {
      lock = new InterProcessSemaphoreMutex(curator, path);
      locks.put(path, lock);
    }
    try {
      // if the lock is out, fail immediately
      gotLock = lock.acquire(MILLISECONDS_TO_WAIT, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.warn("Failure communicating with Zookeeper", e);
    }

    return gotLock;
  }

  /**
   * A method that blocks until the lock for the requested dataset becomes available.
   *
   * @param datasetKey the dataset on which to lock
   *
   * @throws org.gbif.api.exception.ServiceUnavailableException
   *          if there is an error communicating with Zookeeper
   */
  public void waitForLock(String datasetKey) {
    checkNotNull(datasetKey);

    String path = buildPath(datasetKey);

    InterProcessSemaphoreMutex lock = locks.get(path);
    if (lock == null) {
      lock = new InterProcessSemaphoreMutex(curator, path);
      locks.put(path, lock);
    }
    try {
      lock.acquire();
    } catch (Exception e) {
      throw new ServiceUnavailableException("Failure while communicating with Zookeeper", e);
    }
  }

  /**
   * Release a held lock on a dataset. There is no danger in calling this method if the lock is not held.
   *
   * @param datasetKey the dataset for which the held lock should be released
   */
  public void releaseLock(String datasetKey) {
    String path = buildPath(datasetKey);
    InterProcessSemaphoreMutex lock = locks.get(path);
    try {
      if (lock != null) {
        lock.release();
      }
    } catch (Exception e) {
      LOG.warn("Failure communicating with Zookeeper", e);
    } finally {
      // if we fail to contact zookeeper we've already lost the lock
      locks.remove(path);
    }
  }

  private String buildPath(String datasetKey) {
    return PATH_TO_LOCKS + datasetKey;
  }
}
