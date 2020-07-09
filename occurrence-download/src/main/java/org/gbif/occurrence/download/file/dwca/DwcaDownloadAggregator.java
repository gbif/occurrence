package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.util.HeadersFileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import javax.inject.Inject;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Aggregates partials results of files and combine then into the output zip file.
 */
public class DwcaDownloadAggregator implements DownloadAggregator {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaDownloadAggregator.class);

  // Service that persist dataset usage information
  private final OccurrenceDownloadService occurrenceDownloadService;

  private final DownloadJobConfiguration configuration;


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

  /**
   * Appends the result files to the output file.
   */
  private static void appendResult(Result result, OutputStream interpretedFileWriter, OutputStream verbatimFileWriter,
                                   OutputStream multimediaFileWriter) throws IOException {
    DownloadFileUtils.appendAndDelete(result.getDownloadFileWork().getJobDataFileName()
                                      + TableSuffixes.INTERPRETED_SUFFIX, interpretedFileWriter);
    DownloadFileUtils.appendAndDelete(result.getDownloadFileWork().getJobDataFileName() + TableSuffixes.VERBATIM_SUFFIX,
                                      verbatimFileWriter);
    DownloadFileUtils.appendAndDelete(result.getDownloadFileWork().getJobDataFileName()
                                      + TableSuffixes.MULTIMEDIA_SUFFIX, multimediaFileWriter);
  }

  @Autowired
  public DwcaDownloadAggregator(OccurrenceDownloadService occurrenceDownloadService,
                                DownloadJobConfiguration configuration) {
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.configuration = configuration;
  }

  public void init() {
    createFile(configuration.getInterpretedDataFileName());
    createFile(configuration.getVerbatimDataFileName());
    createFile(configuration.getMultimediaDataFileName());
  }

  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  @Override
  public void aggregate(List<Result> results) {
    init();
    try (
      FileOutputStream interpretedFileWriter = new FileOutputStream(configuration.getInterpretedDataFileName(), true);
      FileOutputStream verbatimFileWriter = new FileOutputStream(configuration.getVerbatimDataFileName(), true);
      FileOutputStream multimediaFileWriter = new FileOutputStream(configuration.getMultimediaDataFileName(), true)) {

      HeadersFileUtil.appendInterpretedHeaders(interpretedFileWriter);
      HeadersFileUtil.appendVerbatimHeaders(verbatimFileWriter);
      HeadersFileUtil.appendMultimediaHeaders(multimediaFileWriter);
      if (!results.isEmpty()) {
        // Results are sorted to respect the original ordering
        Collections.sort(results);
        DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
        for (Result result : results) {
          datasetUsagesCollector.sumUsages(result.getDatasetUsages());
          appendResult(result, interpretedFileWriter, verbatimFileWriter, multimediaFileWriter);
        }
        CitationsFileWriter.createCitationFile(datasetUsagesCollector.getDatasetUsages(),
                                               configuration.getCitationDataFileName(),
                                               occurrenceDownloadService,
                                               configuration.getDownloadKey());
      }
      //Creates the DwcA zip file
      DwcaArchiveBuilder.buildArchive(configuration);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

}
