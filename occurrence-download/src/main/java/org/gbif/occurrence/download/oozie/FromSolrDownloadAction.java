package org.gbif.occurrence.download.oozie;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.OccurrenceDownloadExecutor;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that encapsulates the process of creating the occurrence files from Solr/HBase.
 * To start the process
 */
public class FromSolrDownloadAction {

  private static final Logger LOG = LoggerFactory.getLogger(FromSolrDownloadAction.class);

  private static WorkflowConfiguration workflowConfiguration;
  /**
   * Executes the download creation process.
   * All the arguments are required and expected in the following order:
   *  0. downloadFormat: output format
   *  1. solrQuery: Solr query to produce to be used to retrieve the results.
   *  2. downloadKey: occurrence download identifier.
   *  3. filter: filter predicate.
   *  4. downloadTableName: base table/file name.
   */
  public static void main(String[] args) throws Exception {
    Properties settings = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    settings.setProperty(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_FORMAT_KEY,args[0]);
    workflowConfiguration = new WorkflowConfiguration(settings);
    run(new DownloadJobConfiguration.Builder()
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
  public static void run(DownloadJobConfiguration configuration)
    throws IOException {
    final Injector injector = createInjector(configuration);
    CuratorFramework curator = injector.getInstance(CuratorFramework.class);
    final OccurrenceDownloadExecutor
      downloadFileSupervisor = injector.getInstance(OccurrenceDownloadExecutor.class);

    downloadFileSupervisor.run();
    curator.close();
  }



  /**
   * Utility method that creates the Guice injector.
   */
  private static Injector createInjector(DownloadJobConfiguration configuration) {
    try {
      return Guice.createInjector(new DownloadWorkflowModule(workflowConfiguration,configuration));
    } catch (IllegalArgumentException e) {
      LOG.error("Error initializing injection module", e);
      throw Throwables.propagate(e);
    }
  }

}
