package org.gbif.occurrence.download.file.oozie;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.file.OccurrenceFileWriter;
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
 * Class that wraps the process of creating the occurrence and citation files.
 * This class can be executed as jvm application that receives the following arguments:
 * - occurrence data outputFile
 * - citationFileName
 * - solr query
 * - hadoop name node, required to access the hdfs.
 * - hadoop dfs output directory where the citation and data files will be copied
 */
public class DownloadTablesAction {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadTablesAction.class);

  /**
   * Entry point, receives the following arguments:
   * - occurrence interpreted data output file
   * - occurrence verbatim data output file
   * - citationFileName
   * - solr query
   * - hadoop name node, required to access the hdfs.
   * - hadoop dfs output directory where the citation and data files will be copied
   */
  public static void main(String[] args) throws IOException {
    DownloadTablesAction downloadTablesAction = new DownloadTablesAction();
    downloadTablesAction.run(args[0],
                             DownloadFormat.valueOf(args[1]),
                             args[2],
                             args[3],
                             args[4],
                             DownloadUtils.workflowToDownloadId(args[5]));
  }

  /**
   * Executes the file creation process.
   * Citation and data files are created in the local file system and the moved to hadoop file system directory
   * 'hdfsPath'.
   */
  public static void run(String tableBaseName, DownloadFormat downloadFormat,
    String query, String nameNode, String hdfsOutputPath, String downloadId)
    throws IOException {
    final Injector injector = createInjector(downloadId, downloadFormat,hdfsOutputPath, nameNode);
    CuratorFramework curator = injector.getInstance(CuratorFramework.class);
    OccurrenceFileWriter occurrenceFileWriter = injector.getInstance(OccurrenceFileWriter.class);
    occurrenceFileWriter.run(tableBaseName, query, downloadFormat);
    curator.close();
  }



  /**
   * Utility method that creates the Guice injector.
   */
  private static Injector createInjector(String downloadKey, DownloadFormat downloadFormat, String hdfsOutputPath, String nameNode) {
    try {
      Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
      properties.put(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_KEY,downloadKey);
      properties.put(DownloadWorkflowModule.DynamicSettings.HDFS_OUPUT_PATH_KEY,hdfsOutputPath);
      return Guice.createInjector(new DownloadWorkflowModule(properties));
    } catch (IllegalArgumentException e) {
      LOG.error("Error initializing injection module", e);
      Throwables.propagate(e);
    } catch (IOException e) {
      LOG.error("Error initializing injection module", e);
      Throwables.propagate(e);
    }
    throw new IllegalStateException("Guice couldn't be initialized");
  }

}
