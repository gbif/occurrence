package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SearchQueryProcessor;
import org.gbif.occurrence.download.hive.DownloadTerms;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

import akka.actor.UntypedActor;
import com.google.common.base.Throwables;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import static org.gbif.occurrence.download.file.OccurrenceMapReader.buildOccurrenceMap;

/**
 * Actor that creates a part of the simple csv download file.
 */
public class SimpleCsvDownloadActor extends UntypedActor {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvDownloadActor.class);

  static {
    //https://issues.apache.org/jira/browse/BEANUTILS-387
    ConvertUtils.register(new DateConverter(null), Date.class);
  }

  private static final String[] COLUMNS = DownloadTerms.SIMPLE_DOWNLOAD_TERMS.stream()
    .map(Term::simpleName).toArray(String[]::new);


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
  private void doWork(DownloadFileWork work) throws IOException {

    final DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();

    try (ICsvMapWriter csvMapWriter = new CsvMapWriter(new FileWriterWithEncoding(work.getJobDataFileName(),
                                                                                  StandardCharsets.UTF_8),
                                                       CsvPreference.TAB_PREFERENCE)) {

      SearchQueryProcessor.processQuery(work, occurrenceKey -> {
          try {
            org.apache.hadoop.hbase.client.Result result = work.getOccurrenceMapReader().get(occurrenceKey);
            Map<String, String> occurrenceRecordMap = buildOccurrenceMap(result, DownloadTerms.SIMPLE_DOWNLOAD_TERMS);
            if (occurrenceRecordMap != null) {
              //collect usages
              datasetUsagesCollector.collectDatasetUsage(occurrenceRecordMap.get(GbifTerm.datasetKey.simpleName()),
                      occurrenceRecordMap.get(DcTerm.license.simpleName()));
              //write results
              csvMapWriter.write(occurrenceRecordMap, COLUMNS);
            } else {
              LOG.error(String.format("Occurrence id %s not found!", occurrenceKey));
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      );
    } finally {
      // Release the lock
      work.getLock().unlock();
      LOG.info("Lock released, job detail: {} ", work);
    }
    getSender().tell(new Result(work, datasetUsagesCollector.getDatasetUsages(),
      datasetUsagesCollector.getDatasetLicenses()), getSelf());
  }

}
