package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.occurrence.download.citations.CitationsFileReader;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.FileJob;
import org.gbif.occurrence.download.file.OccurrenceDownloadFileCoordinator;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.utils.file.FileUtils;
import org.gbif.wrangler.lock.Lock;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.inject.Inject;

import akka.dispatch.Await;
import akka.dispatch.Future;
import akka.util.Duration;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the creation of the zip file that contains a CSV file with the occurrence data.
 */
public class SimpleCsvOccurrenceDownloadFileCoordinator implements OccurrenceDownloadFileCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvOccurrenceDownloadFileCoordinator.class);

  private static final String CSV_EXTENSION = ".csv";

  private final DownloadJobConfiguration configuration;
  private final WorkflowConfiguration workflowConfiguration;
  private final String outputFileName;

  @Inject
  public SimpleCsvOccurrenceDownloadFileCoordinator(DownloadJobConfiguration configuration, WorkflowConfiguration workflowConfiguration){
    this.configuration = configuration;
    this.workflowConfiguration = workflowConfiguration;
    outputFileName = configuration.getDownloadTempDir() + Path.SEPARATOR + configuration.getDownloadKey() + CSV_EXTENSION;

  }
  @Override
  public void init(){
    try {
      Files.createFile(Paths.get(outputFileName));
    } catch (Throwable t){
      LOG.error("Error creating files",t);
      throw  Throwables.propagate(t);
    }
  }
  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  @Override
  public void aggregateResults(Future<Iterable<Result>> futures)
    throws Exception {
    List<Result> results =
      Lists.newArrayList(Await.result(futures, Duration.Inf()));
    if (!results.isEmpty()) {
      mergeResults(results);
      FileSystem fileSystem = DownloadFileUtils.getHdfs(workflowConfiguration.getHdfsNameNode());
      SimpleCsvArchiveBuilder.mergeToZip(FileSystem.getLocal(new Configuration()).getRawFileSystem(),
                                         fileSystem,
                                         configuration.getDownloadTempDir(),
                                         workflowConfiguration.getHdfsOutputPath(),
                                         configuration.getDownloadKey(),
                                         ModalZipOutputStream.MODE.DEFAULT);
      //Delete the temp directory
      FileUtils.deleteDirectoryRecursively(Paths.get(configuration.getDownloadTempDir()).toFile());
    }
  }

  /**
   * Merges the files of each job into a single CSV file.
   */
  private void mergeResults(List<Result> results) throws IOException {
    try (FileOutputStream outputFileWriter =
           new FileOutputStream(outputFileName, true)) {
      // Results are sorted to respect the original ordering
      Collections.sort(results);
      DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
      for (Result result : results) {
        datasetUsagesCollector.sumUsages(result.getDatasetUsages());
        DownloadFileUtils.appendAndDelete(result.getFileJob().getJobDataFileName(), outputFileWriter);
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
    CitationsFileReader.PersistUsage persistUsage = new CitationsFileReader.PersistUsage(workflowConfiguration.getRegistryWsUrl());
    for(Map.Entry<UUID,Long> usage :  datasetUsagesCollector.getDatasetUsages().entrySet()){
      DatasetOccurrenceDownloadUsage datasetOccurrenceDownloadUsage = new DatasetOccurrenceDownloadUsage();
      datasetOccurrenceDownloadUsage.setNumberRecords(usage.getValue());
      datasetOccurrenceDownloadUsage.setDatasetKey(usage.getKey());
      datasetOccurrenceDownloadUsage.setDownloadKey(configuration.getDownloadKey());
      persistUsage.apply(datasetOccurrenceDownloadUsage);
    }
  }

  /**
   * Builds a new instance of a SimpleCsvFileWriterJob.
   */
  @Override
  public Callable<Result> createJob(FileJob fileJob, Lock lock, SolrServer solrServer, OccurrenceMapReader occurrenceMapReader){
    return new SimpleCsvFileWriterJob(fileJob, lock, solrServer, occurrenceMapReader);
  }

}
