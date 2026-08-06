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
package org.gbif.occurrence.download.file.specieslist;

import static org.gbif.occurrence.download.file.OccurrenceMapReader.selectTerms;

import java.util.Date;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpeciesListDownloadWorker<T extends Occurrence> implements DownloadFileWorker {
  private static final Logger LOG = LoggerFactory.getLogger(SpeciesListDownloadWorker.class);

  private final SearchQueryProcessor<T, OccurrenceSearchParameter> searchQueryProcessor;

  private final Function<T, Map<String,String>> interpretedMapper;

  public SpeciesListDownloadWorker(
      SearchQueryProcessor<T, OccurrenceSearchParameter> searchQueryProcessor,
      Function<T, Map<String, String>> interpretedMapper) {
    this.searchQueryProcessor = searchQueryProcessor;
    this.interpretedMapper = interpretedMapper;
  }

  static {
    // https://issues.apache.org/jira/browse/BEANUTILS-387
    ConvertUtils.register(new DateConverter(null), Date.class);
  }

  /**
   * Executes the job.query and creates a data file that will contain the records from job.from to
   * job.to positions.
   */
  @Override
  public Result work(DownloadFileWork work) {

    DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
    SpeciesListCollector speciesCollector = new SpeciesListCollector();
    try {
      searchQueryProcessor.processQuery(work, occurrence -> {
        Map<String, String> occurrenceRecordMap = selectTerms(interpretedMapper.apply(occurrence), DownloadTerms.SPECIES_LIST_TERMS);
        if (occurrenceRecordMap != null) {
          // collect usages
          datasetUsagesCollector.collectDatasetUsage(occurrenceRecordMap.get(GbifTerm.datasetKey.simpleName()),
              occurrenceRecordMap.get(DcTerm.license.simpleName()));
          speciesCollector.collect(occurrenceRecordMap);
        }
      });

      return new SpeciesListResult(work, datasetUsagesCollector.getDatasetUsages(), datasetUsagesCollector.getDatasetLicenses(),
        speciesCollector.getDistinctSpecies());
    } finally {
      // Release the lock
      work.getLock().unlock();
      LOG.info("Lock released, job detail: {} ", work);
    }
  }
}
