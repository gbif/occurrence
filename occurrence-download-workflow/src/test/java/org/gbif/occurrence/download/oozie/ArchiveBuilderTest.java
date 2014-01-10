package org.gbif.occurrence.download.oozie;

import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.utils.file.FileUtils;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.mockito.Mock;

/**
 *
 */
public class ArchiveBuilderTest {
  @Mock
  private DatasetService datasetService;
  @Mock
  private DatasetOccurrenceDownloadUsageService datasetUsageService;

  @Test
  public void testBuildDescriptor() throws Exception {
    final String downloadID = "0007978-131106143450413";
    final File archiveDir = FileUtils.createTempDir();
    Configuration conf = new Configuration();
    FileSystem localfs = FileSystem.getLocal(conf);

    System.out.println("Writing test meta.xml to " + archiveDir.toString());
    ArchiveBuilder ab = new ArchiveBuilder(downloadID, "testuser", "query", datasetService, datasetUsageService,
                                           conf, localfs, localfs, archiveDir,
                                           "dataTable", "citationTable", "hdfsPath", "http://down.io", true);
    ab.addArchiveDescriptor();
  }
}
