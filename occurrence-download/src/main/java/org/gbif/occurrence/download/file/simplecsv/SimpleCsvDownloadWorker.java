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
package org.gbif.occurrence.download.file.simplecsv;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.DownloadFileWorker;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SearchQueryProcessor;
import org.gbif.occurrence.download.hive.DownloadTerms;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.supercsv.encoder.DefaultCsvEncoder;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import static org.gbif.occurrence.download.file.OccurrenceMapReader.populateVerbatimCsvFields;
import static org.gbif.occurrence.download.file.OccurrenceMapReader.selectTerms;

/**
 * Worker that creates a part of the simple csv download file.
 */
@Slf4j
public class SimpleCsvDownloadWorker<T extends Occurrence> implements DownloadFileWorker {

  private final SearchQueryProcessor<T, OccurrenceSearchParameter> searchQueryProcessor;

  private final Function<T, Map<String,String>> interpretedRecordMapper;

  public SimpleCsvDownloadWorker(SearchQueryProcessor<T, OccurrenceSearchParameter> searchQueryProcessor,
                                Function<T, Map<String,String>> interpretedRecordMapper) {
    this.searchQueryProcessor = searchQueryProcessor;
    this.interpretedRecordMapper = interpretedRecordMapper;
  }

  static {
    //https://issues.apache.org/jira/browse/BEANUTILS-387
    ConvertUtils.register(new DateConverter(null), Date.class);
  }

  private static final String[] COLUMNS = DownloadTerms.SIMPLE_DOWNLOAD_TERMS.stream()
    .map(DownloadTerms::simpleName)
    .toArray(String[]::new);

  /**
   * Executes the job.query and creates a data file that will contain the records from job.from to job.to positions.
   */
  @Override
  public Result work(DownloadFileWork work) throws IOException {

    final DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();

    CsvPreference preference =
        new CsvPreference.Builder(CsvPreference.TAB_PREFERENCE)
            .useEncoder(new DefaultCsvEncoder())
            .build();

    try (ICsvMapWriter csvMapWriter =
        new CsvMapWriter(
            new OutputStreamWriter(new FileOutputStream(work.getJobDataFileName()), StandardCharsets.UTF_8),
            preference)) {

      searchQueryProcessor.processQuery(work, record -> {
          Map<String, String> recordMap = selectTerms(interpretedRecordMapper.apply(record), DownloadTerms.SIMPLE_DOWNLOAD_TERMS);
          populateVerbatimCsvFields(recordMap, record);

          //collect usages
          datasetUsagesCollector.collectDatasetUsage(recordMap.get(GbifTerm.datasetKey.simpleName()),
                                                     recordMap.get(DcTerm.license.simpleName()));
          //write results
          try {
            csvMapWriter.write(recordMap, COLUMNS);
            csvMapWriter.flush();
          } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
          }
        }
      );

      return new Result(work, datasetUsagesCollector.getDatasetUsages(),
        datasetUsagesCollector.getDatasetLicenses());
    } finally {
      // Release the lock
      work.getLock().unlock();
      log.info("Lock released, job detail: {} ", work);
    }
  }

}
