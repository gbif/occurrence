/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.oozie;

import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oozie Action to take a snapshot of the HDFS View directory.
 * It uses a Zookeeper/Curator barrier to synchronize the access to that directory.
 */
public class HdfsSnapshotAction {

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

  private static final Logger LOG = LoggerFactory.getLogger(HdfsSnapshotAction.class);

  public static void main(String[] args) throws IOException {
    Properties settings = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);

    //Action
    Action action = Action.valueOf(args[0].toUpperCase());

    //Hdfs vew directory
    String directory = args[1];

    //Workflow Id, it will be used as the snapshot name
    String wfId = args[2];

    doInBarrier(settings, action, directory, wfId);
  }

  /**
   * Creates a non-started instance of {@link CuratorFramework}.
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
   * @param directory to snapshot
   * @param wfId workflow Id, it is used as the snapshot name
   */
  private static void doInBarrier(Properties config, Action action, String directory, String wfId) {
    try(CuratorFramework curator = curator(config)) {
      curator.start();
      String lockPath = config.getProperty(LOCK_PATH) + config.getProperty(LOCK_NAME);
      DistributedBarrier barrier = new DistributedBarrier(curator, lockPath);
      if(Action.START == action) {
        LOG.info("Waiting for barrier {}", lockPath);
        barrier.waitOnBarrier();
        LOG.info("Setting barrier {}", lockPath);
        barrier.setBarrier();
        createHdfsSnapshot(directory, wfId);
        LOG.info("Removing barrier {}", lockPath);
        barrier.removeBarrier();
      } else if(Action.END == action) {
        LOG.info("Removing barrier {}", lockPath);
        barrier.removeBarrier();
        deleteHdfsSnapshot(directory, wfId);
      } else {
        LOG.error("No action performed");
      }
    } catch (Exception ex) {
      LOG.error("Error handling barrier {}", config);
    }
  }

  /**
   * Create a HDFS Snapshot to the input directory.
   */
  private static Path createHdfsSnapshot(String directory, String snapshotName) throws IOException {
    try (FileSystem fs = FileSystem.get(getHadoopConf())) {
        try {
          return fs.createSnapshot(new Path(directory), snapshotName);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
    }
  }

  /**
   * Deletes a HDFS Snapshot to the input directory.
   */
  private static void deleteHdfsSnapshot(String directory, String snapshotName) throws IOException {
    try (FileSystem fs =FileSystem.get(getHadoopConf())) {
      fs.deleteSnapshot(new Path(directory), snapshotName);
    }
  }

  /**
   * Gets the Hadoop Configurations. Oozie make a file available with all Hadoop (hdfs and yarn) configs.
   */
  private static Configuration getHadoopConf() {
    Configuration actionConf = new Configuration(false);
    actionConf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
    return actionConf;
  }


}
