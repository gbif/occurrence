package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.occurrence.download.citations.CitationsFileReader;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.utils.file.FileUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.inject.Inject;

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Combine the parts created by actor and combine them into single zip file.
 */
public class SimpleCsvDownloadAggregator implements DownloadAggregator {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvDownloadAggregator.class);

  private static final String CSV_EXTENSION = ".csv";

  private final DownloadJobConfiguration configuration;
  private final WorkflowConfiguration workflowConfiguration;
  private final String outputFileName;

  @Inject
  public SimpleCsvDownloadAggregator(
    DownloadJobConfiguration configuration, WorkflowConfiguration workflowConfiguration
  ) {
    this.configuration = configuration;
    this.workflowConfiguration = workflowConfiguration;
    outputFileName =
      configuration.getDownloadTempDir() + Path.SEPARATOR + configuration.getDownloadKey() + CSV_EXTENSION;

  }

  public void init() {
    try {
      Files.createFile(Paths.get(outputFileName));
    } catch (Throwable t) {
      LOG.error("Error creating files", t);
      throw Throwables.propagate(t);
    }
  }

  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  @Override
  public void aggregate(List<Result> results) {
    //init();
    try {
      if (!results.isEmpty()) {
        mergeResults(results);
      }
      SimpleCsvArchiveBuilder.mergeToZip(FileSystem.getLocal(new Configuration()).getRawFileSystem(),
                                         DownloadFileUtils.getHdfs(workflowConfiguration.getHdfsNameNode()),
                                         configuration.getDownloadTempDir(),
                                         workflowConfiguration.getHdfsOutputPath(),
                                         configuration.getDownloadKey(),
                                         ModalZipOutputStream.MODE.DEFAULT);
      //Delete the temp directory
      FileUtils.deleteDirectoryRecursively(Paths.get(configuration.getDownloadTempDir()).toFile());
    } catch (IOException ex) {
      LOG.error("Error aggregating download files", ex);
      throw Throwables.propagate(ex);
    }
  }

  /**
   * Merges the files of each job into a single CSV file.
   */
  private void mergeResults(List<Result> results) {
    try (FileOutputStream outputFileWriter = new FileOutputStream(outputFileName, true)) {
      // Results are sorted to respect the original ordering
      Collections.sort(results);
      DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
      for (Result result : results) {
        datasetUsagesCollector.sumUsages(result.getDatasetUsages());
        DownloadFileUtils.appendAndDelete(result.getDownloadFileWork().getJobDataFileName(), outputFileWriter);
      }
      persistUsages(datasetUsagesCollector);
    } catch (Exception e) {
      LOG.error("Error merging results", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Persists the dataset usages collected in by the datasetUsagesCollector.
   */
  private void persistUsages(DatasetUsagesCollector datasetUsagesCollector) {
    CitationsFileReader.PersistUsage persistUsage =
      new CitationsFileReader.PersistUsage(workflowConfiguration.getRegistryWsUrl());
    for (Map.Entry<UUID, Long> usage : datasetUsagesCollector.getDatasetUsages().entrySet()) {
      DatasetOccurrenceDownloadUsage datasetOccurrenceDownloadUsage = new DatasetOccurrenceDownloadUsage();
      datasetOccurrenceDownloadUsage.setNumberRecords(usage.getValue());
      datasetOccurrenceDownloadUsage.setDatasetKey(usage.getKey());
      datasetOccurrenceDownloadUsage.setDownloadKey(configuration.getDownloadKey());
      persistUsage.apply(datasetOccurrenceDownloadUsage);
    }
  }

}
