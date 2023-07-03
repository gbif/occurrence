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

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SearchQueryProcessor;
import org.gbif.occurrence.download.hive.DownloadTerms;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.common.base.Throwables;

import akka.actor.AbstractActor;

import static org.gbif.occurrence.download.file.OccurrenceMapReader.populateVerbatimCsvFields;
import static org.gbif.occurrence.download.file.OccurrenceMapReader.selectTerms;

/**
 * Actor that creates a part of the simple csv download file.
 */
public class SimpleCsvDownloadActor<T extends Occurrence> extends AbstractActor {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvDownloadActor.class);

  private final SearchQueryProcessor<T> searchQueryProcessor;

  private final Function<T, Map<String,String>> interpretedRecordMapper;

  public SimpleCsvDownloadActor(SearchQueryProcessor<T> searchQueryProcessor,
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

  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(DownloadFileWork.class, this::doWork)
      .build();
  }

  /**
   * Executes the job.query and creates a data file that will contain the records from job.from to job.to positions.
   */
  private void doWork(DownloadFileWork work) throws IOException {

    final DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();

    try (ICsvMapWriter csvMapWriter = new CsvMapWriter(new FileWriterWithEncoding(work.getJobDataFileName(),
                                                                                  StandardCharsets.UTF_8),
                                                       CsvPreference.TAB_PREFERENCE)) {

      searchQueryProcessor.processQuery(work, record -> {
          try {
            Map<String, String> recordMap = selectTerms(interpretedRecordMapper.apply(record), DownloadTerms.SIMPLE_DOWNLOAD_TERMS);
            populateVerbatimCsvFields(recordMap, record);

            //collect usages
            datasetUsagesCollector.collectDatasetUsage(recordMap.get(GbifTerm.datasetKey.simpleName()),
                                                       recordMap.get(DcTerm.license.simpleName()));
            //write results
            csvMapWriter.write(recordMap, COLUMNS);

          } catch (Exception e) {
            getSender().tell(e, getSelf()); // inform our master
            throw Throwables.propagate(e);
          }
        }
      );

      getSender().tell(new Result(work, datasetUsagesCollector.getDatasetUsages(),
        datasetUsagesCollector.getDatasetLicenses()), getSelf());
    } finally {
      // Release the lock
      work.getLock().unlock();
      LOG.info("Lock released, job detail: {} ", work);
    }
  }

}
