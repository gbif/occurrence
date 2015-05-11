package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.occurrence.download.file.FileJob;
import org.gbif.occurrence.download.file.OccurrenceDownloadConfiguration;
import org.gbif.occurrence.download.file.OccurrenceDownloadFileCoordinator;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.util.HeadersFileUtil;
import org.gbif.wrangler.lock.Lock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import javax.inject.Inject;

import akka.dispatch.Await;
import akka.dispatch.Future;
import akka.util.Duration;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.inject.name.Named;
import org.apache.solr.client.solrj.SolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DwcaOccurrenceDownloadFileCoordinator implements OccurrenceDownloadFileCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaOccurrenceDownloadFileCoordinator.class);

  // Service that persist dataset usage information
  private final DatasetOccurrenceDownloadUsageService datasetOccUsageService;

  //Dataset service
  private final DatasetService datasetService;

  private OccurrenceDownloadConfiguration configuration;


  @Inject
  public DwcaOccurrenceDownloadFileCoordinator(
    DatasetOccurrenceDownloadUsageService datasetOccUsageService,
    DatasetService datasetService
  ){
    this.datasetService = datasetService;
    this.datasetOccUsageService = datasetOccUsageService;
  }


  /**
   * Utility method that creates a file, if the files exists it is deleted.
   */
  private static void createFile(String outFile) {
    try {
      File file = new File(outFile);
      if (file.exists()) {
        file.delete();
      }
      file.createNewFile();
    } catch (IOException e) {
      LOG.error("Error creating file", e);
      throw Throwables.propagate(e);
    }

  }

  public void init(OccurrenceDownloadConfiguration configuration){
    this.configuration = configuration;
    createFile(configuration.getInterpretedDataFileName());
    createFile(configuration.getVerbatimDataFileName());
    createFile(configuration.getMultimediaDataFileName());
  }
  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  public void aggregateResults(Future<Iterable<Result>> futures)
    throws IOException {
    Closer outFileCloser = Closer.create();
    try {
      List<Result> results =
        Lists.newArrayList(Await.result(futures, Duration.Inf()));
      if (!results.isEmpty()) {
        // Results are sorted to respect the original ordering
        Collections.sort(results);
        FileOutputStream interpretedFileWriter =
          outFileCloser.register(new FileOutputStream(configuration.getInterpretedDataFileName(), true));
        FileOutputStream verbatimFileWriter =
          outFileCloser.register(new FileOutputStream(configuration.getVerbatimDataFileName(), true));
        FileOutputStream multimediaFileWriter =
          outFileCloser.register(new FileOutputStream(configuration.getMultimediaDataFileName(), true));
        HeadersFileUtil.appendInterpretedHeaders(interpretedFileWriter);
        HeadersFileUtil.appendVerbatimHeaders(verbatimFileWriter);
        HeadersFileUtil.appendMultimediaHeaders(multimediaFileWriter);
        DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
        for (Result result : results) {
          datasetUsagesCollector.sumUsages(result.getDatasetUsages());
          appendResult(result, interpretedFileWriter, verbatimFileWriter, multimediaFileWriter);
        }
        CitationsFileWriter.createCitationFile(datasetUsagesCollector.getDatasetUsages(),
                                               configuration.getCitationDataFileName(),
                                               datasetOccUsageService,
                                               datasetService, configuration.getDownloadKey());

        DwcaArchiveBuilder.buildArchive(configuration);
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      outFileCloser.close();
    }
  }

  private void createArchive(){

  }

  public Callable<Result> createJob(FileJob fileJob, Lock lock, SolrServer solrServer, OccurrenceMapReader occurrenceMapReader){
    return new OccurrenceFileWriterJob(fileJob, lock, solrServer, occurrenceMapReader);
  }

  /**
   * Appends a result file to the output file.
   */
  private static void appendResult(Result result, OutputStream interpretedFileWriter, OutputStream verbatimFileWriter,
                            OutputStream multimediaFileWriter)
    throws IOException {
    DownloadFileUtils.appendAndDelete(result.getFileJob().getJobDataFileName() + Constants.INTERPRETED_SUFFIX, interpretedFileWriter);
    DownloadFileUtils.appendAndDelete(result.getFileJob().getJobDataFileName() + Constants.VERBATIM_SUFFIX, verbatimFileWriter);
    DownloadFileUtils.appendAndDelete(result.getFileJob().getJobDataFileName() + Constants.MULTIMEDIA_SUFFIX, multimediaFileWriter);
  }

}
