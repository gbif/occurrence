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

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.DownloadMaster;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.wrangler.lock.Mutex;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

/**
 * Class that encapsulates the process of creating the occurrence files from Elasticsearch/Hive.
 * To start the process
 */
public class FromSearchDownloadAction {

  private static final Logger LOG = LoggerFactory.getLogger(FromSearchDownloadAction.class);

  private static final long SLEEP_TIME_BEFORE_TERMINATION = 5000L;

  /**
   * Private constructor.
   */
  private FromSearchDownloadAction() {
    //Instances of this class are not allowed
  }

  /**
   * Executes the download creation process.
   * All the arguments are required and expected in the following order:
   * 0. downloadFormat: output format
   * 1. searchQuery: Search query to produce to be used to retrieve the results.
   * 2. downloadKey: occurrence download identifier.
   * 3. filter: filter predicate.
   * 4. downloadTableName: base table/file name.
   */
  public static void main(String[] args) throws Exception {
    Properties settings = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    settings.setProperty(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_FORMAT_KEY, args[0]);
    WorkflowConfiguration workflowConfiguration = new WorkflowConfiguration(settings);
    DwcTerm coreTerm =  DwcTerm.valueOf(args[6]);
    Set<Extension> extensions = Arrays.stream(args[7].split(",")).map(Extension::valueOf).collect(Collectors.toSet());
    run(workflowConfiguration, DownloadJobConfiguration.builder()
          .searchQuery(args[1])
          .downloadKey(args[2])
          .filter(args[3])
          .downloadTableName(args[4])
          .sourceDir(workflowConfiguration.getTempDir())
          .isSmallDownload(true)
          .downloadFormat(workflowConfiguration.getDownloadFormat())
          .user(args[5])
          .coreTerm(coreTerm)
          .extensions(extensions)
          .build());

  }

  /**
   * This method it's mirror of the 'main' method, is kept for clarity in parameters usage.
   */
  public static void run(WorkflowConfiguration workflowConfiguration, DownloadJobConfiguration configuration) {

    DownloadWorkflowModule module = DownloadWorkflowModule.builder()
      .workflowConfiguration(workflowConfiguration)
      .downloadJobConfiguration(configuration)
      .build();

    try (CuratorFramework curatorIndices = module.curatorFramework()) {

      // Create an Akka system
      ActorSystem system = ActorSystem.create("DownloadSystem" + configuration.getDownloadKey());

      // create the master
      ActorRef master = module.downloadMaster(system);

      Mutex readMutex = module.provideReadLock(curatorIndices);
      readMutex.acquire();
      // start the calculation
      master.tell(new DownloadMaster.Start());
      while (!master.isTerminated()) {
        try {
          Thread.sleep(SLEEP_TIME_BEFORE_TERMINATION);
        } catch (InterruptedException ie) {
          LOG.error("Thread interrupted", ie);
        }
      }
      system.shutdown();
      readMutex.release();
    }
  }

}
