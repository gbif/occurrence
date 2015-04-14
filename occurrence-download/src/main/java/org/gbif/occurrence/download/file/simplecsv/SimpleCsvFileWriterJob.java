package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.occurrence.download.file.FileJob;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DatasetUsagesCollector;
import org.gbif.occurrence.download.file.common.SolrQueryProcessor;
import org.gbif.occurrence.download.hive.DownloadTableDefinitions;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.wrangler.lock.Lock;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
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
 * Job that creates a part of CSV file. The file is generated according to the fileJob field.
 */
class SimpleCsvFileWriterJob implements Callable<Result> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvFileWriterJob.class);

  static {
    //https://issues.apache.org/jira/browse/BEANUTILS-387
    ConvertUtils.register(new DateConverter(null), Date.class);
  }

  private static final String[] COLUMNS = DownloadTableDefinitions.simpleDownload().toArray(new String[0]);


  // Default page size for Solr queries.
  private static final int LIMIT = 300;

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
           new CsvMapWriter(new FileWriterWithEncoding(fileJob.getBaseDataFileName() + '/' + fileJob.getJobDataFileName(), Charsets.UTF_8),
                            CsvPreference.TAB_PREFERENCE)) {


      SolrQueryProcessor.processQuery(fileJob,solrServer, new Predicate<Integer>() {
        @Override
        public boolean apply(@Nullable Integer occurrenceKey) {
          try {
            org.apache.hadoop.hbase.client.Result result = occurrenceMapReader.get(occurrenceKey);
            Map<String, String> occurrenceRecordMap = OccurrenceMapReader.buildOccurrenceMap(result, DownloadTerms.SimpleDownload.SIMPLE_DOWNLOAD_TERMS);
            if (occurrenceRecordMap != null) {
              datasetUsagesCollector.collectUsage(occurrenceRecordMap);
              csvMapWriter.write(occurrenceRecordMap, COLUMNS);
              return true;
            } else {
              LOG.error(String.format("Occurrence id %s not found!", occurrenceKey));
            }
          } catch (Exception e) {
            Throwables.propagate(e);
          }
          return false;
        }
      });
    } finally {
      // Unlock the assigned lock.
      lock.unlock();
      LOG.info("Lock released, job detail: {} ", fileJob.toString());
    }
    return new Result(fileJob, datasetUsagesCollector.getDatasetUsages());
  }


}
