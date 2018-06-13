package org.gbif.occurrence.processor.zookeeper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryUntilElapsed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple wrapper around a DistributedAtomicLong that will flush periodically to ZooKeeper. It's meant as a
 * patch to solve the massive contention problems that DALs have when many threads try to increment at the same time.
 * Note that counts could be lost if the client application dies.
 */
public final class BatchingDalWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(BatchingDalWrapper.class);

  private final CuratorFramework client;
  private final List<String> paths = Lists.newArrayList();

  public BatchingDalWrapper(CuratorFramework client, long flushFrequencyMsecs) {
    this.client = client;
    new Thread(new Flusher(flushFrequencyMsecs)).start();
  }

  public void increment(String path) {
    synchronized (paths) {
      paths.add(path);
    }
  }

  private class Flusher implements Runnable {

    private final long flushFrequencyMsecs;

    private Flusher(long flushFrequencyMsecs) {
      this.flushFrequencyMsecs = flushFrequencyMsecs;
    }

    @Override
    public void run() {
      while (true) {
        List<String> copy;
        synchronized (paths) {
          copy = Lists.newArrayList(paths);
          paths.clear();
        }

        // merge additions
        Map<String, AtomicLong> mutations = Maps.newHashMap();
        for (String path : copy) {
          if (mutations.containsKey(path)) {
            mutations.get(path).incrementAndGet();
          } else {
            mutations.put(path, new AtomicLong(1));
          }
        }
        for (Map.Entry<String, AtomicLong> entry : mutations.entrySet()) {
          try {
            String path = entry.getKey();
            String basePath = path.substring(0, 44); // TODO: UGLY!
            LOG.debug("Path {} exists? {}", basePath, client.checkExists().forPath(basePath));
            if (client.checkExists().forPath(basePath) == null) {
              LOG.warn("Counter {} no longer exists, not changing it, counts will be wrong", path);
            } else {
              DistributedAtomicLong dal = new DistributedAtomicLong(client, path,
                new RetryUntilElapsed((int) TimeUnit.MINUTES.toMillis(5), (int) TimeUnit.MILLISECONDS.toMillis(25)));
              AtomicValue<Long> result = dal.add(entry.getValue().get());
              if (!result.succeeded()) {
                LOG.warn("Counter updates are failing and we've exhausted retry - counts will be wrong");
              }
            }
          } catch (Exception e) {
            LOG.warn("Failed to update DALs during flush - counts will be wrong", e);
          }
        }
        try {
          Thread.sleep(flushFrequencyMsecs); // not a true time of course
        } catch (InterruptedException e1) {
          break; // really?
        }
      }
    }
  }
}
