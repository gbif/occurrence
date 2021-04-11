package org.gbif.occurrence.download.file.dwca.oozie;

import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.dwca.DwcaArchiveBuilder;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oozie Java action that creates a DwCA from Hive tables.
 * This action is used for big DwCA downloads.
 */
public class ArchiveDownloadAction {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveDownloadAction.class);

  /**
   * Entry point for assembling the DWC archive.
   * The thrown exception is the only way of telling Oozie that this job has failed.
   *
   * @throws java.io.IOException if any read/write operation failed
   */
  public static void main(String[] args) throws IOException {
    String downloadKey = args[0];
    String username = args[1];          // download user
    String query = args[2];         // download query filter
    boolean isSmallDownload = Boolean.parseBoolean(args[3]);    // isSmallDownload
    String downloadTableName = args[4];    // download table/file name

    WorkflowConfiguration workflowConfiguration = new WorkflowConfiguration();

    DownloadJobConfiguration configuration = new DownloadJobConfiguration.Builder()
      .withDownloadKey(downloadKey)
      .withDownloadTableName(downloadTableName)
      .withFilter(query)
      .withIsSmallDownload(isSmallDownload)
      .withUser(username)
      .withSourceDir(workflowConfiguration.getHiveDBPath())
      .withDownloadFormat(workflowConfiguration.getDownloadFormat())
      .build();

    LOG.info("DwcaArchiveBuilder instance created with parameters:{}", (Object)args);
    DwcaArchiveBuilder.buildArchive(configuration, workflowConfiguration);
  }

  /**
   * Hidden constructor.
   */
  private ArchiveDownloadAction() {
    //empty constructor
  }
}
