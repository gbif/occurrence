package org.gbif.occurrence.cli.common;

import org.gbif.occurrence.common.config.ZooKeeperConfiguration;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZkUtils {

  private ZkUtils() {
  }

  /**
   * This method returns a connection object to ZooKeeper with the provided settings and creates and starts a {@link
   * org.apache.curator.framework.CuratorFramework}. These settings are not validated in this method so only call it
   * when the object has been
   * validated.
   *
   * @return started CuratorFramework
   *
   * @throws java.io.IOException if connection fails
   */
  public static CuratorFramework newCuratorFramework(ZooKeeperConfiguration cfg) throws IOException {
    CuratorFramework curator = CuratorFrameworkFactory.builder()
      .namespace(cfg.namespace)
      .retryPolicy(new ExponentialBackoffRetry(cfg.baseSleepTime, cfg.maxRetries))
      .connectString(cfg.connectionString).build();
    curator.start();
    return curator;
  }
}
