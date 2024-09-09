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
package org.gbif.occurrence.table.backfill;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Paths;

/**
 * Oozie Action to take a snapshot of the HDFS View directory.
 * It uses a Zookeeper/Curator barrier to synchronize the access to that directory.
 */
@Slf4j
@Data
@AllArgsConstructor
public class HdfsSnapshotCoordinator {

  private final TableBackfillConfiguration configuration;

  private final Configuration hadoopConfiguration;

  public static final String CONF_FILE = "download.yaml";


  /**
   * Creates a non-started instance of {@link CuratorFramework}.
   */
  private CuratorFramework curator() {
    return CuratorFrameworkFactory.builder().namespace(configuration.getHdfsLock().getNamespace())
      .retryPolicy(new ExponentialBackoffRetry(configuration.getHdfsLock().getConnectionSleepTimeMs(),
                                              configuration.getHdfsLock().getConnectionMaxRetries()))
      .connectString(configuration.getHdfsLock().getZkConnectionString())
      .build();
  }

  /**
   * Performs the START/SET or END/REMOVE on a barrier based on the action.
   * @param directory to snapshot
   * @param snapshotName workflow Id, it is used as the snapshot name
   */
  public void createHdfsSnapshot(String snapshotName) {
    try(CuratorFramework curator = curator()) {
      FileSystem fs = FileSystem.get(hadoopConfiguration);
      curator.start();
      String lockPath = configuration.getHdfsLock().getPath() + configuration.getHdfsLock().getName();
      DistributedBarrier barrier = new DistributedBarrier(curator, lockPath);
      log.info("Waiting for barrier {}", lockPath);
      barrier.waitOnBarrier();
      log.info("Setting barrier {}", lockPath);
      barrier.setBarrier();
      Path snapshotPath = fs.createSnapshot(new Path(configuration.getSourceDirectory(), configuration.getCoreName().toLowerCase()), snapshotName);
      log.info("Snapshot created {}", snapshotPath);
      log.info("Removing barrier {}", lockPath);
      barrier.removeBarrier();
    } catch (Exception ex) {
      log.error("Error handling barrier {}", configuration);
      throw new RuntimeException(ex);
    }
  }


  /**
   * Performs the START/SET or END/REMOVE on a barrier based on the action.
   * @param action action to be performed
   * @param directory to snapshot
   * @param snapshotName workflow Id, it is used as the snapshot name
   */
  public void deleteHdfsSnapshot(String snapshotName) {
    try( CuratorFramework curator = curator()) {
      log.info("Deleting snapshot {}", snapshotName);
      FileSystem fs = FileSystem.get(hadoopConfiguration);
      curator.start();
      String lockPath = configuration.getHdfsLock().getPath() + configuration.getHdfsLock().getName();
      DistributedBarrier barrier = new DistributedBarrier(curator, lockPath);
      log.info("Removing barrier {}", lockPath);
      barrier.removeBarrier();
      fs.deleteSnapshot(new Path(configuration.getSourceDirectory(), configuration.getCoreName().toLowerCase()), snapshotName);
    } catch (Exception ex) {
      log.error("Error handling barrier {}", configuration);
      throw new RuntimeException(ex);
    }
  }

  public static String getSnapshotPath(TableBackfillConfiguration configuration,  String dataDirectory, String jobId) {
    String path =
      Paths.get(
          configuration.getSourceDirectory(),
          configuration.getCoreName().toLowerCase(),
          ".snapshot",
          jobId,
          dataDirectory.toLowerCase())
        .toString();
    log.info("Snapshot path {}", path);
    return path;
  }
}
