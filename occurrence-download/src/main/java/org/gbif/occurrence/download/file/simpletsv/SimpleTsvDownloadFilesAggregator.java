package org.gbif.occurrence.download.file.simpletsv;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.citations.CitationsFileReader;
import org.gbif.occurrence.download.file.DownloadFilesAggregator;
import org.gbif.occurrence.download.file.FileJob;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.util.HeadersFileUtil;
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
import com.google.common.io.Closer;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.solr.client.solrj.SolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTsvDownloadFilesAggregator implements DownloadFilesAggregator {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleTsvDownloadFilesAggregator.class);

  private final String nameNode;

  private final String hdfsOutputPath;

  private final String registryWsUrl;

  private final String downloadKey;


  @Inject
  public SimpleTsvDownloadFilesAggregator(
    @Named(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY) String nameNode,
    @Named(DownloadWorkflowModule.DynamicSettings.HDFS_OUPUT_PATH_KEY) String hdfsOutputPath,
    @Named(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY) String registryWsUrl,
    @Named(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_KEY) String downloadKey
  ){
    this.hdfsOutputPath = hdfsOutputPath;
    this.nameNode = nameNode;
    this.registryWsUrl = registryWsUrl;
    this.downloadKey = downloadKey;
  }


  public void init(String baseTableName, DownloadFormat downloadFormat){
    try {
      Files.createFile(Paths.get(baseTableName));
    } catch (Throwable t){
      Throwables.propagate(t);
    }
  }
  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  public void aggregateResults(Future<Iterable<Result>> futures, String baseTableName)
    throws IOException {
    Closer outFileCloser = Closer.create();
    try {
      List<Result> results =
        Lists.newArrayList(Await.result(futures, Duration.Inf()));
      if (!results.isEmpty()) {
        // Results are sorted to respect the original ordering
        Collections.sort(results);
        FileOutputStream outputFileWriter =
          outFileCloser.register(new FileOutputStream(baseTableName, true));
        HeadersFileUtil.appendInterpretedHeaders(outputFileWriter);
        DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
        for (Result result : results) {
          datasetUsagesCollector.sumUsages(result.getDatasetUsages());
          DownloadFileUtils.appendAndDelete(result.getFileJob().getBaseTableName(), outputFileWriter);
        }
        FileSystem fileSystem = DownloadFileUtils.getHdfs(nameNode);
        SimpleTsvArchiveBuilder.mergeToZip(FileSystem.getLocal(new Configuration()).getRawFileSystem(), fileSystem, baseTableName, hdfsOutputPath,
                                           downloadKey,
                                           ModalZipOutputStream.MODE.DEFAULT);
        persistUsages(datasetUsagesCollector);
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      outFileCloser.close();
    }
  }

  private void persistUsages(DatasetUsagesCollector datasetUsagesCollector){
    CitationsFileReader.PersistUsage persistUsage = new CitationsFileReader.PersistUsage(registryWsUrl);
    for(Map.Entry<UUID,Long> usage :  datasetUsagesCollector.getDatasetUsages().entrySet()){
      DatasetOccurrenceDownloadUsage datasetOccurrenceDownloadUsage = new DatasetOccurrenceDownloadUsage();
      datasetOccurrenceDownloadUsage.setNumberRecords(usage.getValue());
      datasetOccurrenceDownloadUsage.setDatasetKey(usage.getKey());
      datasetOccurrenceDownloadUsage.setDownloadKey(DownloadUtils.workflowToDownloadId(downloadKey));
      persistUsage.apply(datasetOccurrenceDownloadUsage);
    }
  }

  public Callable<Result> createJob(FileJob fileJob, Lock lock, SolrServer solrServer, OccurrenceMapReader occurrenceMapReader){
    return new SimpleTsvFileWriterJob(fileJob, lock, solrServer, occurrenceMapReader);
  }

}
