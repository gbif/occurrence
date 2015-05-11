package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.occurrence.download.citations.CitationsFileReader;
import org.gbif.occurrence.download.file.FileJob;
import org.gbif.occurrence.download.file.OccurrenceDownloadConfiguration;
import org.gbif.occurrence.download.file.OccurrenceDownloadFileCoordinator;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
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
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.solr.client.solrj.SolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleCsvOccurrenceDownloadFileCoordinator implements OccurrenceDownloadFileCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvOccurrenceDownloadFileCoordinator.class);

  private static final String CSV_EXTENSION = ".csv";

  private final String nameNode;

  private final String hdfsOutputPath;

  private final String registryWsUrl;

  private final String downloadKey;

  private OccurrenceDownloadConfiguration configuration;


  @Inject
  public SimpleCsvOccurrenceDownloadFileCoordinator(
    @Named(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY) String nameNode,
    @Named(DownloadWorkflowModule.DefaultSettings.HDFS_OUPUT_PATH_KEY) String hdfsOutputPath,
    @Named(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY) String registryWsUrl,
    @Named(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_KEY) String downloadKey
  ){
    this.hdfsOutputPath = hdfsOutputPath;
    this.nameNode = nameNode;
    this.registryWsUrl = registryWsUrl;
    this.downloadKey = downloadKey;
  }


  private static String getOutputFileName(String baseDataFileName, String extension){
    return getOutputFileName(baseDataFileName) + extension;
  }

  private static String getOutputFileName(String baseDataFileName){
    return baseDataFileName + "/" + baseDataFileName;
  }

  @Override
  public void init(OccurrenceDownloadConfiguration configuration){
    try {
      this.configuration = configuration;
      Files.createDirectory(Paths.get(configuration.getDownloadKey()));
      Files.createFile(Paths.get(getOutputFileName(configuration.getDownloadKey(),CSV_EXTENSION)));
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
      FileSystem fileSystem = DownloadFileUtils.getHdfs(nameNode);
      SimpleCsvArchiveBuilder.mergeToZip(FileSystem.getLocal(new Configuration()).getRawFileSystem(),
                                         fileSystem,
                                         configuration.getDownloadKey(),
                                         hdfsOutputPath,
                                         downloadKey,
                                         ModalZipOutputStream.MODE.DEFAULT);
      FileUtils.deleteDirectoryRecursively(Paths.get(configuration.getDownloadKey()).toFile());
    }
  }

  /**
   * Merges the files of each job into a single CSV file.
   */
  private void mergeResults(List<Result> results) throws IOException {
    try (FileOutputStream outputFileWriter =
           new FileOutputStream(getOutputFileName(configuration.getDownloadKey(), CSV_EXTENSION), true)) {
      // Results are sorted to respect the original ordering
      Collections.sort(results);
      DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
      for (Result result : results) {
        datasetUsagesCollector.sumUsages(result.getDatasetUsages());
        DownloadFileUtils.appendAndDelete(Paths.get(configuration.getDownloadKey(), result.getFileJob().getJobDataFileName())
                                            .toString(), outputFileWriter);
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
    CitationsFileReader.PersistUsage persistUsage = new CitationsFileReader.PersistUsage(registryWsUrl);
    for(Map.Entry<UUID,Long> usage :  datasetUsagesCollector.getDatasetUsages().entrySet()){
      DatasetOccurrenceDownloadUsage datasetOccurrenceDownloadUsage = new DatasetOccurrenceDownloadUsage();
      datasetOccurrenceDownloadUsage.setNumberRecords(usage.getValue());
      datasetOccurrenceDownloadUsage.setDatasetKey(usage.getKey());
      datasetOccurrenceDownloadUsage.setDownloadKey(downloadKey);
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
