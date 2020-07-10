package org.gbif.occurrence.download.oozie;

import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.DownloadMaster;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.util.Properties;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.wrangler.lock.Mutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

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
    run(workflowConfiguration, new DownloadJobConfiguration.Builder().withSearchQuery(args[1])
          .withDownloadKey(args[2])
          .withFilter(args[3])
          .withDownloadTableName(args[4])
          .withSourceDir(workflowConfiguration.getTempDir())
          .withIsSmallDownload(true)
          .withDownloadFormat(workflowConfiguration.getDownloadFormat())
          .withUser(args[5])
          .build());

  }

  /**
   * This method it's mirror of the 'main' method, is kept for clarity in parameters usage.
   */
  public static void run(WorkflowConfiguration workflowConfiguration, DownloadJobConfiguration configuration) {
    ApplicationContext applicationContext = DownloadWorkflowModule.buildAppContext(workflowConfiguration, configuration);

    try (CuratorFramework curatorDownload = applicationContext.getBean("Downloads", CuratorFramework.class);
         CuratorFramework curatorIndices = applicationContext.getBean("Indices", CuratorFramework.class)) {

      // Create an Akka system
      ActorSystem system = ActorSystem.create("DownloadSystem" + configuration.getDownloadKey());

      // create the master
      ActorRef master = applicationContext.getBean(DownloadMaster.MasterFactory.class).build(system);

      Mutex readMutex = applicationContext.getBean(Mutex.class);
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

