package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SolrQueryProcessor;
import org.gbif.occurrence.download.hive.DownloadTerms;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import javax.annotation.Nullable;

import akka.actor.UntypedActor;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
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

  private static final String[] COLUMNS =
    Collections2.transform(DownloadTerms.SIMPLE_DOWNLOAD_TERMS, new Function<Term, String>() {
                             @Nullable
                             @Override
                             public String apply(@Nullable Term input) {
                               return input.simpleName();
                             }
                           }).toArray(new String[DownloadTerms.SIMPLE_DOWNLOAD_TERMS.size()]);

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

    try (ICsvMapWriter csvMapWriter = new CsvMapWriter(new FileWriterWithEncoding(work.getJobDataFileName(),
                                                                                  Charsets.UTF_8),
                                                       CsvPreference.TAB_PREFERENCE)) {

      SolrQueryProcessor.processQuery(work, new Predicate<Integer>() {
        @Override
        public boolean apply(@Nullable Integer occurrenceKey) {
          try {
            org.apache.hadoop.hbase.client.Result result = work.getOccurrenceMapReader().get(occurrenceKey);
            Map<String, String> occurrenceRecordMap = buildOccurrenceMap(result, DownloadTerms.SIMPLE_DOWNLOAD_TERMS);
            if (occurrenceRecordMap != null) {
              //collect usages
              datasetUsagesCollector.incrementDatasetUsage(occurrenceRecordMap.get(GbifTerm.datasetKey.simpleName()));
              //write results
              csvMapWriter.write(occurrenceRecordMap, COLUMNS);
              return true;
            } else {
              LOG.error(String.format("Occurrence id %s not found!", occurrenceKey));
            }
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
          return false;
        }
      });
    } finally {
      // Release the lock
      work.getLock().unlock();
      LOG.info("Lock released, job detail: {} ", work.toString());
    }
    getSender().tell(new Result(work, datasetUsagesCollector.getDatasetUsages()), getSelf());
  }

}
