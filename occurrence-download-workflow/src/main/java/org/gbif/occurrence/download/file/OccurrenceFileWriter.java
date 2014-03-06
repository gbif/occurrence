package org.gbif.occurrence.download.file;

import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.common.search.util.SolrConstants;
import org.gbif.occurrence.download.util.HeadersFileUtil;
import org.gbif.wrangler.lock.Lock;
import org.gbif.wrangler.lock.LockFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import akka.dispatch.Await;
import akka.dispatch.ExecutionContextExecutorService;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import akka.util.Duration;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that creates a single CSV file using a OccurrenceSearchRequest.
 * The file is built with a set of worker threads that create a piece of the output file, once the jobs have finished
 * their jobs a single file is created. The order of the records is respected from the search request by using the
 * fileJob.startOffset reported by each job.
 */
public class OccurrenceFileWriter {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceFileWriter.class);

  /**
   * Utility class that holds the general settings of the OccurrenceFileWriter class.
   */
  public static class Configuration {

    // Maximum number of workers
    private final int nrOfWorkers;

    // Minimum number of records per job
    private final int minNrOfRecords;

    // Limits the maximum number of records that can be processed.
    // This parameters avoids use this class to create file with a size beyond a maximum.
    private final int maximumNrOfRecords;

    // Occurrence download lock/counter name
    private final String lockName;

    /**
     * Default/full constructor.
     */
    @Inject
    public Configuration(@Named("job.max_threads") int nrOfWorkers, @Named("job.min_records") int minNrOfRecords,
      @Named("file.max_records") int maximumNrOfRecords, @Named("zookeeper.lock_name") String lockName) {
      this.nrOfWorkers = nrOfWorkers;
      this.minNrOfRecords = minNrOfRecords;
      this.maximumNrOfRecords = maximumNrOfRecords;
      this.lockName = lockName;
    }

  }

  private static final String FINISH_MSG_FMT = "Time elapsed %d minutes and %d seconds";

  private final SolrServer solrServer;

  private final Configuration conf;

  private final LockFactory lockFactory;

  private final OccurrenceMapReader occurrenceHBaseReader;

  // Service that persist dataset usage information
  private final DatasetOccurrenceDownloadUsageService datasetOccUsageService;

  // Download key
  private final String downloadId;


  /**
   * Default constructor.
   */
  @Inject
  public OccurrenceFileWriter(LockFactory lockFactory,
    Configuration configuration, SolrServer solrServer, OccurrenceMapReader occurrenceHBaseReader,
    DatasetOccurrenceDownloadUsageService datasetOccUsageService, @Named("downloadId") String downloadId) {
    this.conf = configuration;
    this.lockFactory = lockFactory;
    this.solrServer = solrServer;
    this.occurrenceHBaseReader = occurrenceHBaseReader;
    this.datasetOccUsageService = datasetOccUsageService;
    this.downloadId = downloadId;
  }

  /**
   * Utility method that creates a file, if the files exists it is deleted.
   */
  private static void createFile(String outFile) {
    try {
      File file = new File(outFile);
      if (file.exists()) {
        file.delete();
      }
      file.createNewFile();
    } catch (IOException e) {
      LOG.error("Error creating file", e);
      Throwables.propagate(e);
    }

  }


  /**
   * Entry point. This method: creates the output files, runs the jobs (OccurrenceFileWriterJob) and then collect the
   * results. Individual files created by each job are deleted.
   */
  public void run(String interpretedOutFile, String verbatimOutFile, String citationFileName, String query) {
    try {
      StopWatch stopwatch = new StopWatch();
      stopwatch.start();
      ExecutionContextExecutorService executionContext =
        ExecutionContexts.fromExecutorService(Executors.newFixedThreadPool(conf.nrOfWorkers));
      collectResults(Futures.sequence(
        runJobs(executionContext, interpretedOutFile, verbatimOutFile, query),
        executionContext), interpretedOutFile, verbatimOutFile, citationFileName);
      stopwatch.stop();
      executionContext.shutdownNow();
      final long timeInSeconds = TimeUnit.MILLISECONDS.toSeconds(stopwatch.getTime());
      LOG.info(String.format(FINISH_MSG_FMT, TimeUnit.SECONDS.toMinutes(timeInSeconds), timeInSeconds % 60));
    } catch (IOException e) {
      LOG.info("Error creating occurrence file", e);
      Throwables.propagate(e);
    }
  }

  /**
   * Appends a result file to the output file.
   */
  private void appendResult(Result result, OutputStream interpretedFileWriter, OutputStream verabtimFileWriter)
    throws IOException {
    Closer closer = Closer.create();
    File interpretedDataFile = new File(result.getFileJob().getInterpretedDataFile());
    File verbatimDataFile = new File(result.getFileJob().getVerbatimDataFile());
    try {
      FileInputStream interpretedFileReader = closer.register(new FileInputStream(interpretedDataFile));
      FileInputStream verbatimFileReader = closer.register(new FileInputStream(verbatimDataFile));
      ByteStreams.copy(interpretedFileReader, interpretedFileWriter);
      ByteStreams.copy(verbatimFileReader, verabtimFileWriter);
    } catch (FileNotFoundException e) {
      LOG.info("Error creating occurrence files", e);
      Throwables.propagate(e);
    } finally {
      closer.close();
      interpretedDataFile.delete();
      verbatimDataFile.delete();
    }
  }

  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  private void collectResults(Future<Iterable<Result>> futures, String interpretedOutFile, String verbatimOutFile,
    String citationFileName)
    throws IOException {
    Closer outFileCloser = Closer.create();
    try {
      List<Result> results =
        Lists.newArrayList(Await.result(futures, Duration.Inf()));
      if (!results.isEmpty()) {
        // Results are sorted to respect the original ordering
        Collections.sort(results);
        FileOutputStream interpretedFileWriter =
          outFileCloser.register(new FileOutputStream(interpretedOutFile, true));
        FileOutputStream verbatimFileWriter =
          outFileCloser.register(new FileOutputStream(verbatimOutFile, true));
        HeadersFileUtil.appendInterpretedHeaders(interpretedFileWriter);
        HeadersFileUtil.appendVerbatimHeaders(verbatimFileWriter);
        Map<UUID, Long> datasetUsages = Maps.newHashMap();
        for (Result result : results) {
          datasetUsages = sumUsages(datasetUsages, result.getDatasetUsages());
          appendResult(result, interpretedFileWriter, verbatimFileWriter);
        }
        CitationsFileWriter.createCitationFile(datasetUsages, citationFileName, datasetOccUsageService, downloadId);
      }
    } catch (FileNotFoundException e) {
      Throwables.propagate(e);
    } catch (IOException e) {
      Throwables.propagate(e);
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      outFileCloser.close();
    }
  }


  /**
   * Creates and gets a reference to a Lock.
   */
  private Lock getLock() {
    return lockFactory.makeLock(conf.lockName);
  }

  /**
   * Executes a search and get number of records.
   */
  private Long getSearchCount(String query) {
    try {
      SolrQuery solrQuery = new SolrQuery().setQuery(SolrConstants.DEFAULT_QUERY).setRows(0);
      if (!Strings.isNullOrEmpty(query)) {
        solrQuery.addFilterQuery(query);
      }
      QueryResponse queryResponse = solrServer.query(solrQuery);
      return queryResponse.getResults().getNumFound();
    } catch (SolrServerException e) {
      LOG.error("Error executing Solr", e);
      return 0L;
    }
  }

  /**
   * Run the list of jobs. The amount of records is assigned evenly among the worker threads.
   * If the amount of records is not divisible by the nrOfWorkers the remaining records are assigned "evenly" among the
   * first jobs.
   */
  private List<Future<Result>> runJobs(ExecutionContextExecutorService executionContext, String interpretedOutFile,
    String verbatimOutFile, String query) {
    final int recordCount = getSearchCount(query).intValue();
    if (recordCount <= 0) {
      return Lists.newArrayList();
    }
    final int nrOfRecords = Math.min(recordCount, conf.maximumNrOfRecords);
    // Calculates the required workers.
    int calcNrOfWorkers =
      conf.minNrOfRecords >= nrOfRecords ? 1 : Math.min(conf.nrOfWorkers, nrOfRecords / conf.minNrOfRecords);

    // Number of records that will be assigned to each job
    final int sizeOfChunks = Math.max(nrOfRecords / calcNrOfWorkers, 1);

    // Remaining jobs, that are not assigned to a job yet
    final int remaining = nrOfRecords - (sizeOfChunks * calcNrOfWorkers);

    // How many of the remaining jobs will be assigned to one job
    int remainingPerJob = remaining > 0 ? Math.max(remaining / calcNrOfWorkers, 1) : 0;

    List<Future<Result>> futures = Lists.newArrayList();
    createFile(interpretedOutFile);
    createFile(verbatimOutFile);
    int from;
    int to = 0;
    int additionalJobsCnt = 0;
    for (int i = 0; i < calcNrOfWorkers; i++) {
      from = i == 0 ? 0 : to;
      to = from + sizeOfChunks + remainingPerJob;

      // Calculates the remaining jobs that will be assigned to the new FileJob.
      additionalJobsCnt += remainingPerJob;
      if (remainingPerJob != 0 && additionalJobsCnt > remaining) {
        remainingPerJob = additionalJobsCnt - remaining;
      } else if (additionalJobsCnt == remaining) {
        remainingPerJob = 0;
      }
      FileJob file = new FileJob(from, to, interpretedOutFile + i, verbatimOutFile + i, query);
      // Awaits for an available thread
      Lock lock = getLock();
      LOG.info("Requesting a lock for job {}, detail: {}", i, file.toString());
      lock.lock();
      LOG.info("Lock granted for job {}, detail: {}", i, file.toString());
      // Adds the Job to the list. The file name is the output file name + the sequence i
      futures.add(Futures.future(new OccurrenceFileWriterJob(file, lock, solrServer, occurrenceHBaseReader),
        executionContext));
    }
    return futures;
  }

  /**
   * Adds the integer values of each input map.
   */
  private Map<UUID, Long> sumUsages(Map<UUID, Long> usages1, Map<UUID, Long> usages2) {
    Map<UUID, Long> result = Maps.newHashMap();
    for (Entry<UUID, Long> entry1 : usages1.entrySet()) {
      Long valueIn2 = usages2.get(entry1.getKey());
      if (valueIn2 == null) {
        result.put(entry1.getKey(), entry1.getValue());
      } else {
        result.put(entry1.getKey(), entry1.getValue() + valueIn2);
      }
    }
    result.putAll(Maps.difference(usages1, usages2).entriesOnlyOnRight());
    return result;
  }
}
