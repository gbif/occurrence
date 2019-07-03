package org.gbif.occurrence.download.oozie;

import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oozie Action to START/SET and END/REMOVE Zookeeper/Curator barriers.
 */
public class BarrierAction {

  //Settings read from a properties file
  private static final String PROPERTY_PREFIX = "hdfs.lock.";
  private static final String ZK_CONNECTION_STRING = PROPERTY_PREFIX + "zkConnectionString";
  private static final String LOCK_NAMESPACE = PROPERTY_PREFIX + "namespace";
  private static final String LOCK_PATH = PROPERTY_PREFIX + "path";
  private static final String LOCK_NAME = PROPERTY_PREFIX + "name";
  private static final String LOCK_CONNECTION_SLEEP_TIME_MS = PROPERTY_PREFIX + "sleepTimeMs";
  private static final String LOCK_CONNECTION_MAX_TRIES= PROPERTY_PREFIX + "maxRetries";

  /**
   * Supported actions.
   */
  public enum Action {
    START, END
  }

  private static final Logger LOG = LoggerFactory.getLogger(BarrierAction.class);

  public static void main(String[] args) throws IOException {
    Properties settings = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    Action action = Action.valueOf(args[0].toUpperCase());
    doInBarrier(settings, action);
  }

  /**
   * Creates an non-started instance of {@link CuratorFramework}.
   */
  private static CuratorFramework curator(Properties config) {
    return CuratorFrameworkFactory.builder().namespace(config.getProperty(LOCK_NAMESPACE))
      .retryPolicy(new ExponentialBackoffRetry(Integer.parseInt(config.getProperty(LOCK_CONNECTION_SLEEP_TIME_MS,"100")),
                                               Integer.parseInt(config.getProperty(LOCK_CONNECTION_MAX_TRIES,"5"))))
      .connectString(config.getProperty(ZK_CONNECTION_STRING))
      .build();
  }

  /**
   * Performs the START/SET or END/REMOVE on a barrier based on the action.
   * @param config configuration settings
   * @param action action to be performed
   */
  private static void doInBarrier(Properties config, Action action) {
    try(CuratorFramework curator = curator(config)) {
      curator.start();
      String lockPath = config.getProperty(LOCK_PATH) + config.getProperty(LOCK_NAME);
      DistributedBarrier barrier = new DistributedBarrier(curator, lockPath);
      LOG.info("Waiting for barrier {}", lockPath);
      barrier.waitOnBarrier();
      if(Action.START == action) {
        LOG.info("Setting barrier {}", lockPath);
        barrier.setBarrier();
      } else if(Action.END == action) {
        LOG.info("Removing barrier {}", lockPath);
        barrier.removeBarrier();
      } else {
        LOG.error("No action performed");
      }
    } catch (Exception ex) {
      LOG.error("Error handling barrier {}", config);
    }
  }
}
