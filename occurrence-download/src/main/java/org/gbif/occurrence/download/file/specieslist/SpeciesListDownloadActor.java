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

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SearchQueryProcessor;
import org.gbif.occurrence.download.hive.DownloadTerms;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import akka.actor.UntypedActor;

import static org.gbif.occurrence.download.file.OccurrenceMapReader.selectTerms;

public class SpeciesListDownloadActor<T extends Occurrence> extends UntypedActor {
  private static final Logger LOG = LoggerFactory.getLogger(SpeciesListDownloadActor.class);

  private final SearchQueryProcessor<T> searchQueryProcessor;

  private final Function<T, Map<String,String>> interpretedMapper;

  public SpeciesListDownloadActor(SearchQueryProcessor<T> searchQueryProcessor, Function<T, Map<String,String>> interpretedMapper) {
    this.searchQueryProcessor = searchQueryProcessor;
    this.interpretedMapper = interpretedMapper;
  }

  static {
    // https://issues.apache.org/jira/browse/BEANUTILS-387
    ConvertUtils.register(new DateConverter(null), Date.class);
  }


  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof DownloadFileWork) {
      doWork((DownloadFileWork) message);
    } else {
      unhandled(message);
    }
  }

  /**
   * Executes the job.query and creates a data file that will contains the records from job.from to
   * job.to positions.
   */
  private void doWork(DownloadFileWork work) throws IOException {

    DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
    SpeciesListCollector speciesCollector = new SpeciesListCollector();
    try {
      searchQueryProcessor.processQuery(work, occurrence -> {
        try {
          Map<String, String> occurrenceRecordMap = selectTerms(interpretedMapper.apply(occurrence), DownloadTerms.SPECIES_LIST_TERMS);
          if (occurrenceRecordMap != null) {
            // collect usages
            datasetUsagesCollector.collectDatasetUsage(occurrenceRecordMap.get(GbifTerm.datasetKey.simpleName()),
                occurrenceRecordMap.get(DcTerm.license.simpleName()));
            speciesCollector.collect(occurrenceRecordMap);
          }
        } catch (Exception e) {
          getSender().tell(e, getSelf()); // inform our master
          throw Throwables.propagate(e);
        }
      });

      getSender().tell(new SpeciesListResult(work, datasetUsagesCollector.getDatasetUsages(), datasetUsagesCollector.getDatasetLicenses(),
        speciesCollector.getDistinctSpecies()), getSelf());
    } finally {
      // Release the lock
      work.getLock().unlock();
      LOG.info("Lock released, job detail: {} ", work);
    }
  }
}
