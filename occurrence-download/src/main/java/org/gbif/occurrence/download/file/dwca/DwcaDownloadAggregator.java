/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.Extension;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.occurrence.download.util.HeadersFileUtil;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import lombok.SneakyThrows;

/**
 * Aggregates partials results of files and combine then into the output zip file.
 */
public class DwcaDownloadAggregator implements DownloadAggregator {

  public static class ExtensionFilesWriter implements Closeable {

    private final Map<Extension,FileOutputStream> filesMap;

    private final Map<Extension, ExtensionTable> tableMap;

    public ExtensionFilesWriter(DownloadJobConfiguration configuration) {
      filesMap = configuration.getExtensions().stream()
                  .collect(Collectors.toMap(Function.identity(), ext -> {
                    try {
                      String outFile = configuration.getExtensionDataFileName(new ExtensionTable(ext));
                      LOG.info("Aggregating extension file {}", outFile);
                      return new FileOutputStream(outFile, true);
                    } catch (IOException ex) {
                      throw new RuntimeException(ex);
                    }
                  }));
      tableMap = configuration.getExtensions().stream()
                  .collect(Collectors.toMap(Function.identity(), ExtensionTable::new));
    }

    @Override
    public void close() throws IOException {
      for (FileOutputStream fileOutputStream : filesMap.values()) {
        fileOutputStream.close();
      }
    }

    @SneakyThrows
    public void writerHeaders() {
      for (Map.Entry<Extension,FileOutputStream> entry : filesMap.entrySet()) {
        HeadersFileUtil.appendHeaders(entry.getValue(), HeadersFileUtil.getExtensionInterpretedHeader(tableMap.get(entry.getKey())));
      }
    }

    @SneakyThrows
    public void appendAndDelete(Result result) {
      for (Map.Entry<Extension,FileOutputStream> entry : filesMap.entrySet()) {
        DownloadFileUtils.appendAndDelete(result.getDownloadFileWork().getJobDataFileName() + '_' + tableMap.get(entry.getKey()).getHiveTableName(),
                                          entry.getValue());
      }
    }
  }

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
                                   OutputStream multimediaFileWriter, ExtensionFilesWriter extensionFilesWriter) throws IOException {
    DownloadFileUtils.appendAndDelete(result.getDownloadFileWork().getJobDataFileName()
                                      + TableSuffixes.INTERPRETED_SUFFIX, interpretedFileWriter);
    DownloadFileUtils.appendAndDelete(result.getDownloadFileWork().getJobDataFileName() + TableSuffixes.VERBATIM_SUFFIX,
                                      verbatimFileWriter);
    DownloadFileUtils.appendAndDelete(result.getDownloadFileWork().getJobDataFileName()
                                      + TableSuffixes.MULTIMEDIA_SUFFIX, multimediaFileWriter);
    extensionFilesWriter.appendAndDelete(result);

  }

  public DwcaDownloadAggregator(DownloadJobConfiguration configuration,
                                OccurrenceDownloadService occurrenceDownloadService) {
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
      FileOutputStream multimediaFileWriter = new FileOutputStream(configuration.getMultimediaDataFileName(), true);
      ExtensionFilesWriter extensionFilesWriter = new ExtensionFilesWriter(configuration)) {

      HeadersFileUtil.appendInterpretedHeaders(interpretedFileWriter);
      HeadersFileUtil.appendVerbatimHeaders(verbatimFileWriter);
      HeadersFileUtil.appendMultimediaHeaders(multimediaFileWriter);
      extensionFilesWriter.writerHeaders();
      if (!results.isEmpty()) {
        // Results are sorted to respect the original ordering
        Collections.sort(results);
        DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
        for (Result result : results) {
          datasetUsagesCollector.sumUsages(result.getDatasetUsages());
          appendResult(result, interpretedFileWriter, verbatimFileWriter, multimediaFileWriter, extensionFilesWriter);
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
