package org.gbif.occurrence.download.oozie;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.OccurrenceDownloadConfiguration;
import org.gbif.occurrence.download.file.OccurrenceDownloadFileSupervisor;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;

import java.io.IOException;

import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that wraps the process of creating the occurrence files.
 * The parameter 'DownloadFormat' defines the OccurrenceDownloadFileCoordinator to be used.
 */
public class DownloadTablesAction {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadTablesAction.class);

  private static WorkflowConfiguration workflowConfiguration = new WorkflowConfiguration();
  /**
   * Executes the download creation process.
   * All the arguments are required and expected in the following order:
   *  0. downloadFormat: output format
   *  1. solrQuery: Solr query to produce to be used to retrieve the results.
   *  2. hdfsOutputPath: path where the resulting file will be stored.
   *  3. downloadKey: occurrence download identifier.
   */
  public static void main(String[] args) throws Exception {
    run(new OccurrenceDownloadConfiguration.Builder()
      .withDownloadFormat(DownloadFormat.valueOf(args[0]))
      .withSolrQuery(args[1])
      .withDownloadKey(args[2])
      .withFilter(args[3])
      .withDownloadTableName(args[4])
      .withSourceDir(workflowConfiguration.getTempDir())
      .withIsSmallDownload(true).build());

  }

  /**
   * This method it's mirror of the 'main' method, is kept for clarity in parameters usage.
   */
  public static void run(OccurrenceDownloadConfiguration configuration)
    throws IOException {
    final Injector injector = createInjector(configuration);
    CuratorFramework curator = injector.getInstance(CuratorFramework.class);
    final OccurrenceDownloadFileSupervisor
      downloadFileSupervisor = injector.getInstance(OccurrenceDownloadFileSupervisor.class);

    downloadFileSupervisor.run();
    curator.close();
  }



  /**
   * Utility method that creates the Guice injector.
   */
  private static Injector createInjector(OccurrenceDownloadConfiguration configuration) {
    try {
      return Guice.createInjector(new DownloadWorkflowModule(workflowConfiguration,configuration));
    } catch (IllegalArgumentException e) {
      LOG.error("Error initializing injection module", e);
      throw Throwables.propagate(e);
    }
  }

}
