package org.gbif.occurrence.download.oozie;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.file.OccurrenceDownloadFileSupervisor;
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
 * Class that wraps the process of creating the occurrence files.
 * The parameter 'DownloadFormat' defines the OccurrenceDownloadFileCoordinator to be used.
 */
public class DownloadTablesAction {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadTablesAction.class);

  /**
   * Executes the download creation process.
   * All the arguments are required and expected in the following order:
   *  0. downloadFormat: output format
   *  1. solrQuery: Solr query to produce to be used to retrieve the results.
   *  2. hdfsOutputPath: path where the resulting file will be stored.
   *  3. downloadKey: occurrence download identifier.
   */
  public static void main(String[] args) throws IOException {
    run(DownloadFormat.valueOf(args[0]), //downloadFormat
                             args[1], //solrQuery
                             args[2], // hdfsOutputPath
                             args[3]); //downloadKey
  }

  /**
   * This method it's mirror of the 'main' method, is kept for clarity in parameters usage.
   */
  public static void run(DownloadFormat downloadFormat, String query,String hdfsOutputPath, String downloadKey)
    throws IOException {
    final Injector injector = createInjector(downloadKey, downloadFormat,hdfsOutputPath);
    CuratorFramework curator = injector.getInstance(CuratorFramework.class);
    final OccurrenceDownloadFileSupervisor
      downloadFileSupervisor = injector.getInstance(OccurrenceDownloadFileSupervisor.class);
    downloadFileSupervisor.run(downloadKey, query, downloadFormat);
    curator.close();
  }



  /**
   * Utility method that creates the Guice injector.
   */
  private static Injector createInjector(String downloadKey, DownloadFormat downloadFormat, String hdfsOutputPath) {
    try {
      Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
      properties.put(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_KEY,downloadKey);
      properties.put(DownloadWorkflowModule.DynamicSettings.HDFS_OUPUT_PATH_KEY,hdfsOutputPath);
      properties.put(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_FORMAT_KEY,downloadFormat.name());
      return Guice.createInjector(new DownloadWorkflowModule(properties));
    } catch (IllegalArgumentException e) {
      LOG.error("Error initializing injection module", e);
      throw Throwables.propagate(e);
    } catch (IOException e) {
      LOG.error("Error initializing injection module", e);
      throw Throwables.propagate(e);
    }
  }

}
