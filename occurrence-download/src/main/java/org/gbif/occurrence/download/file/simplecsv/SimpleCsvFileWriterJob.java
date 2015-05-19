package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.download.file.FileJob;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SolrQueryProcessor;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.wrangler.lock.Lock;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.solr.client.solrj.SolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

/**
 * Job that creates a part of the download CSV file. The file is generated according to the fileJob field.
 */
class SimpleCsvFileWriterJob implements Callable<Result> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvFileWriterJob.class);

  static {
    //https://issues.apache.org/jira/browse/BEANUTILS-387
    ConvertUtils.register(new DateConverter(null), Date.class);
  }

  private static final String[] COLUMNS = Collections2.transform(DownloadTerms.SIMPLE_DOWNLOAD_TERMS, new Function<Term, String>() {
    @Nullable
    @Override
    public String apply(@Nullable Term input) {
      return input.simpleName();
    }
  }).toArray(new String[0]);

  private final FileJob fileJob;

  private final Lock lock;

  private final SolrServer solrServer;

  private final OccurrenceMapReader occurrenceMapReader;

  /**
   * Default constructor.
   */
  public SimpleCsvFileWriterJob(
    FileJob fileJob, Lock lock, SolrServer solrServer, OccurrenceMapReader occurrenceMapReader
  ) {
    this.fileJob = fileJob;
    this.lock = lock;
    this.solrServer = solrServer;
    this.occurrenceMapReader = occurrenceMapReader;
  }

  /**
   * Executes the job.query and creates a data file that will contains the records from job.from to job.to positions.
   */
  @Override
  public Result call() throws IOException {

    final DatasetUsagesCollector datasetUsagesCollector = new DatasetUsagesCollector();


    try (ICsvMapWriter csvMapWriter =
           new CsvMapWriter(new FileWriterWithEncoding(fileJob.getJobDataFileName(), Charsets.UTF_8),
                            CsvPreference.TAB_PREFERENCE)) {

      SolrQueryProcessor.processQuery(fileJob,solrServer, new Predicate<Integer>() {
        @Override
        public boolean apply(@Nullable Integer occurrenceKey) {
          try {
            org.apache.hadoop.hbase.client.Result result = occurrenceMapReader.get(occurrenceKey);
            Map<String, String> occurrenceRecordMap = OccurrenceMapReader.buildOccurrenceMap(result, DownloadTerms.SIMPLE_DOWNLOAD_TERMS);
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
      lock.unlock();
      LOG.info("Lock released, job detail: {} ", fileJob.toString());
    }
    return new Result(fileJob, datasetUsagesCollector.getDatasetUsages());
  }


}
