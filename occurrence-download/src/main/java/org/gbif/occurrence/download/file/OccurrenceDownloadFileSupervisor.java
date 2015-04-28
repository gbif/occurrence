package org.gbif.occurrence.download.file;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.common.search.util.SolrConstants;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.wrangler.lock.Lock;
import org.gbif.wrangler.lock.LockFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import akka.dispatch.ExecutionContextExecutorService;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
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
 * Coordinates the creation of occurrence download files.
 * Distributes the work among the jobs and coordinates the aggregation of result in a single output interacting with
 * a OccurrenceDownloadFileCoordinator. The order of the records in the  final files is respected from the search
 * request by using the fileJob.startOffset reported by each job.
 */
public class OccurrenceDownloadFileSupervisor {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceDownloadFileSupervisor.class);

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
    public Configuration(@Named(DownloadWorkflowModule.DefaultSettings.MAX_THREADS_KEY) int nrOfWorkers,
                         @Named(DownloadWorkflowModule.DefaultSettings.JOB_MIN_RECORDS_KEY) int minNrOfRecords,
                         @Named(DownloadWorkflowModule.DefaultSettings.MAX_RECORDS_KEY) int maximumNrOfRecords,
                         @Named(DownloadWorkflowModule.DefaultSettings.ZK_LOCK_NAME_KEY) String lockName) {
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

  private final OccurrenceMapReader occurrenceMapReader;

  private final OccurrenceDownloadFileCoordinator occurrenceDownloadFileCoordinator;

  /**
   * Default constructor.
   */
  @Inject
  public OccurrenceDownloadFileSupervisor(
    LockFactory lockFactory,
    Configuration configuration,
    SolrServer solrServer,
    OccurrenceMapReader occurrenceMapReader,
    OccurrenceDownloadFileCoordinator occurrenceDownloadFileCoordinator
  ) {
    conf = configuration;
    this.lockFactory = lockFactory;
    this.solrServer = solrServer;
    this.occurrenceMapReader = occurrenceMapReader;
    this.occurrenceDownloadFileCoordinator = occurrenceDownloadFileCoordinator;
  }


  /**
   * Entry point. This method: creates the output files, runs the jobs (OccurrenceFileWriterJob) and then collect the
   * results. Individual files created by each job are deleted.
   */
  public void run(String baseDataFileName, String query, DownloadFormat downloadFormat) {
    try {
      StopWatch stopwatch = new StopWatch();
      stopwatch.start();
      ExecutionContextExecutorService executionContext =
        ExecutionContexts.fromExecutorService(Executors.newFixedThreadPool(conf.nrOfWorkers));
      occurrenceDownloadFileCoordinator.aggregateResults(Futures.sequence(runJobs(executionContext,
                                                                        baseDataFileName,
                                                                        query,
                                                                        downloadFormat), executionContext),
                                               baseDataFileName);
      stopwatch.stop();
      executionContext.shutdownNow();
      final long timeInSeconds = TimeUnit.MILLISECONDS.toSeconds(stopwatch.getTime());
      LOG.info(String.format(FINISH_MSG_FMT, TimeUnit.SECONDS.toMinutes(timeInSeconds), timeInSeconds % 60));
    } catch (Exception e) {
      LOG.info("Error creating occurrence file", e);
      throw Throwables.propagate(e);
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
      LOG.error("Error executing Solr query", e);
      return 0L;
    }
  }

  /**
   * Run the list of jobs. The amount of records is assigned evenly among the worker threads.
   * If the amount of records is not divisible by the nrOfWorkers the remaining records are assigned "evenly" among the
   * first jobs.
   */
  private List<Future<Result>> runJobs(ExecutionContextExecutorService executionContext, String baseTableName, String query, DownloadFormat downloadFormat) {
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
    occurrenceDownloadFileCoordinator.init(baseTableName, downloadFormat);
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
      FileJob file =
        new FileJob(from, to, baseTableName, i, query);
      // Awaits for an available thread
      Lock lock = getLock();
      LOG.info("Requesting a lock for job {}, detail: {}", i, file.toString());
      lock.lock();
      LOG.info("Lock granted for job {}, detail: {}", i, file.toString());
      // Adds the Job to the list. The file name is the output file name + the sequence i
      futures.add(Futures.future(occurrenceDownloadFileCoordinator.createJob(file, lock, solrServer, occurrenceMapReader),
        executionContext));
    }
    return futures;
  }

}
