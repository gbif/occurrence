package org.gbif.occurrence.download.file.dwca.oozie;

import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.OccurrenceDownloadConfiguration;
import org.gbif.occurrence.download.file.dwca.DwcaArchiveBuilder;

import java.io.IOException;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArchiveDownloadAction {

  private static final Logger LOG = LoggerFactory.getLogger(ArchiveDownloadAction.class);

  /**
   * Entry point for assembling the dwc archive.
   * The thrown exception is the only way of telling Oozie that this job has failed.
   *
   * @throws java.io.IOException if any read/write operation failed
   */
  public static void main(String[] args) throws IOException {
    final String downloadKey = args[0];
    final String username = args[1];          // download user
    final String query = args[2];         // download query filter
    final boolean isSmallDownload = Boolean.parseBoolean(args[3]);    // isSmallDownload
    final String downloadTableName = args[4];    // download table/file name

    WorkflowConfiguration workflowConfiguration = new WorkflowConfiguration();

    OccurrenceDownloadConfiguration configuration = new OccurrenceDownloadConfiguration.Builder()
      .withDownloadKey(downloadKey)
      .withDownloadTableName(downloadTableName)
      .withFilter(query)
      .withIsSmallDownload(isSmallDownload)
      .withUser(username)
      .withSourceDir(workflowConfiguration.getHiveDBPath())
      .build();

    LOG.info("DwcaArchiveBuilder instance created with parameters:{}", Joiner.on(" ").skipNulls().join(args));
    DwcaArchiveBuilder.buildArchive(configuration,workflowConfiguration);
  }
}
