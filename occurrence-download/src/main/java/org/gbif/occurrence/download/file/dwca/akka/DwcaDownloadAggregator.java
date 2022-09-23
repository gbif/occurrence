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
package org.gbif.occurrence.download.file.dwca.akka;

import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.TableSuffixes;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.file.dwca.DwcaArchiveBuilder;
import org.gbif.occurrence.download.util.HeadersFileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Throwables;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Aggregates partials results of files and combine then into the output zip file.
 */
@AllArgsConstructor
@Slf4j
public class DwcaDownloadAggregator implements DownloadAggregator {

  private final DownloadJobConfiguration configuration;

  // Service that persist dataset usage information
  private final OccurrenceDownloadService occurrenceDownloadService;

  /**
   * Utility method that creates a file, if the files exists it is deleted.
   */
  @SneakyThrows
  private static FileOutputStream createFileOutStream(String fileName) {
    File file = new File(fileName);
    Files.deleteIfExists(file.toPath());
    file.createNewFile();
    return new FileOutputStream(fileName, true);
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

  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  @Override
  public void aggregate(List<Result> results) {
    try (
      FileOutputStream interpretedFileWriter = createFileOutStream(configuration.getInterpretedDataFileName());
      FileOutputStream verbatimFileWriter = createFileOutStream(configuration.getVerbatimDataFileName());
      FileOutputStream multimediaFileWriter = createFileOutStream(configuration.getMultimediaDataFileName());
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
        log.info("Usages {}", datasetUsagesCollector.getDatasetUsages());
      }
      //Creates the DwcA zip file
      DwcaArchiveBuilder.of(configuration).buildArchive();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

}
