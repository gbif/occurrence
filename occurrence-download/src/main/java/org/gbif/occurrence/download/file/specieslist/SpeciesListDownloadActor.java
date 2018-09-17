package org.gbif.occurrence.download.file.specieslist;

import static org.gbif.occurrence.download.file.OccurrenceMapReader.buildOccurrenceMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SolrQueryProcessor;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Throwables;
import akka.actor.UntypedActor;

public class SpeciesListDownloadActor extends UntypedActor{
  private static final Logger LOG = LoggerFactory.getLogger(SpeciesListDownloadActor.class);

  static {
    //https://issues.apache.org/jira/browse/BEANUTILS-387
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
   * Executes the job.query and creates a data file that will contains the records from job.from to job.to positions.
   */
  private void doWork(final DownloadFileWork work) throws IOException {

    final DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();
    final SpeciesListCollector speciesCollector = new SpeciesListCollector();
    final List<Map<String,String>> filteredResult = new ArrayList<>();
    try {
      SolrQueryProcessor.processQuery(work, occurrenceKey -> {
        try {
          org.apache.hadoop.hbase.client.Result result =
              work.getOccurrenceMapReader().get(occurrenceKey);
          Map<String, String> occurrenceRecordMap = buildOccurrenceMap(result, DownloadTerms.SPECIES_LIST_TERMS);
          if (occurrenceRecordMap != null) {
            // collect usages
            datasetUsagesCollector.collectDatasetUsage(occurrenceRecordMap.get(GbifTerm.datasetKey.simpleName()), occurrenceRecordMap.get(DcTerm.license.simpleName()));
            filteredResult.add(occurrenceRecordMap);
          } else {
            LOG.error(String.format("Occurrence id %s not found!", occurrenceKey));
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      });
    } finally {
      // Release the lock
      work.getLock().unlock();
      LOG.info("Lock released, job detail: {} ", work);
    }
    speciesCollector.computeDistinctSpecies(filteredResult);
    Result result = new Result(work, datasetUsagesCollector.getDatasetUsages(),
        datasetUsagesCollector.getDatasetLicenses());
    result.setSpeciesListCollector(speciesCollector);
    getSender().tell(result, getSelf());
  }
}
