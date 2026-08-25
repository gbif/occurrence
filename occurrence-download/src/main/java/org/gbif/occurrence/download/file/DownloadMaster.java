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
package org.gbif.occurrence.download.file;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.common.SearchQueryProcessor;
import org.gbif.occurrence.download.file.dwca.DwcaDownloadWorker;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvDownloadWorker;
import org.gbif.occurrence.download.file.specieslist.SpeciesListDownloadWorker;
import org.gbif.occurrence.download.util.Strings;
import org.gbif.search.es.SearchHitConverter;
import org.gbif.search.es.event.EventEsField;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;
import org.gbif.search.es.occurrence.OccurrenceEsResponseParser;
import org.gbif.utils.file.FileUtils;
import org.gbif.wrangler.lock.Lock;
import org.gbif.wrangler.lock.LockFactory;
import org.gbif.wrangler.lock.zookeeper.ZooKeeperLockFactory;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.Path;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Controls the multithreaded creation of occurrence downloads.
 */
@Data
@Slf4j
public class DownloadMaster {

  private static final String RUNNING_JOBS_LOCKING_PATH = "/runningJobs/";

  private static final String FINISH_MSG_FMT = "Time elapsed %d minutes and %d seconds";
  private final ElasticsearchClient esClient;
  private final String esIndex;
  private final MasterConfiguration conf;
  private final CuratorFramework curatorFramework;
  private final LockFactory lockFactory;
  private final DownloadAggregator aggregator;
  private final DownloadJobConfiguration jobConfiguration;

  private final OccurrenceEsFieldMapper occurrenceEsFieldMapper;
  private final Function<Occurrence,Map<String,String>> verbatimMapper;
  private final Function<Occurrence,Map<String,String>> interpretedMapper;
  private final SearchHitConverter<Occurrence> searchHitConverter;
  /** ES field for stable from/size pagination. */
  private final String sortField;


  /**
   * Default constructor.
   */
  @Builder
  public DownloadMaster(
    WorkflowConfiguration workflowConfiguration,
    MasterConfiguration masterConfiguration,
    ElasticsearchClient esClient,
    String esIndex,
    DownloadJobConfiguration jobConfiguration,
    DownloadAggregator aggregator,
    int maxGlobalJobs,
    Function<Occurrence,Map<String,String>> verbatimMapper,
    Function<Occurrence,Map<String,String>> interpretedMapper,
    SearchHitConverter<Occurrence> searchHitConverter) {
    conf = masterConfiguration;
    this.jobConfiguration = jobConfiguration;
    DownloadWorkflowModule downloadWorkflowModule = DownloadWorkflowModule.builder()
      .workflowConfiguration(workflowConfiguration)
      .downloadJobConfiguration(jobConfiguration)
      .build();
    this.curatorFramework = downloadWorkflowModule.curatorFramework();
    this.lockFactory = new ZooKeeperLockFactory(curatorFramework,
                                                maxGlobalJobs,
                                                RUNNING_JOBS_LOCKING_PATH);
    this.esClient = esClient;
    this.esIndex = esIndex;
    this.aggregator = aggregator;
    this.occurrenceEsFieldMapper = OccurrenceEsField.buildFieldMapper();
    this.interpretedMapper = interpretedMapper;
    this.verbatimMapper = verbatimMapper;
    this.searchHitConverter = searchHitConverter;
    this.sortField = sortFieldFor(workflowConfiguration.getEsIndexType());
  }

  static String sortFieldFor(WorkflowConfiguration.SearchType searchType) {
    return searchType == WorkflowConfiguration.SearchType.EVENT
        ? EventEsField.GBIF_ID.getSearchFieldName()
        : OccurrenceEsField.GBIF_ID.getSearchFieldName();
  }

  /**
   * Runs the download job to completion: dispatches the work across a bounded worker pool,
   * aggregates the results into the final archive, and cleans up afterwards.
   * Throws if any worker fails; the caller is responsible for treating that as a failed download.
   */
  public void run() {
    try {
      long startNanos = System.nanoTime();
      log.info("Acquiring Search Index Read Lock");
      File downloadTempDir = new File(jobConfiguration.getDownloadTempDir());
      if (downloadTempDir.exists()) {
        FileUtils.deleteDirectoryRecursively(downloadTempDir);
      }
      downloadTempDir.mkdirs();

      int recordCount = getSearchCount(jobConfiguration.getSearchQuery()).intValue();
      List<Result> results = recordCount > 0 ? runWorkers(recordCount) : new ArrayList<>();
      aggregator.aggregate(results);

      long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
      log.info(String.format(FINISH_MSG_FMT, TimeUnit.MILLISECONDS.toMinutes(elapsedMs), (elapsedMs / 1000) % 60));
    } finally {
      shutdown();
    }
  }

  /**
   * Cleans up external resources held for the duration of the job.
   */
  private void shutdown() {
    shutDownEsClientSilently();
    curatorFramework.close();
  }

  /**
   * Shuts down the ElasticSearch client
   */
  private void shutDownEsClientSilently() {
    try {
      if(Objects.nonNull(esClient)) {
        esClient._transport().close();
      }
    } catch (Exception ex) {
      log.error("Error shutting down Elasticsearch client", ex);
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
      Query esQuery =
          !Strings.isNullOrEmpty(query)
              ? Query.of(q -> q.withJson(new StringReader(query)))
              : Query.of(q -> q.matchAll(m -> m));
      SearchResponse<Void> searchResponse =
          esClient.search(
              s -> s.index(esIndex).size(0).trackTotalHits(t -> t.enabled(true)).query(esQuery),
              Void.class);
      return searchResponse.hits().total() != null ? searchResponse.hits().total().value() : 0L;
    } catch (Exception e) {
      log.error("Error executing query", e);
      return 0L;
    }
  }

  /**
   * Runs the list of jobs across a bounded worker pool and returns their collected results.
   * The amount of records is assigned evenly among the worker threads.
   * If the amount of records is not divisible by the number of workers the remaining records are assigned "evenly" among
   * the first jobs.
   */
  private List<Result> runWorkers(int recordCount) {
    int nrOfRecords = Math.min(recordCount, conf.maximumNrOfRecords);
    // Calculates the required workers.
    int calcNrOfWorkers =
      conf.minNrOfRecords >= nrOfRecords ? 1 : Math.min(conf.nrOfWorkers, nrOfRecords / conf.minNrOfRecords);

    ExecutorService executor = Executors.newFixedThreadPool(calcNrOfWorkers);
    try {
      List<Future<Result>> futures = new ArrayList<>(calcNrOfWorkers);

      // Number of records that will be assigned to each job
      int sizeOfChunks = Math.max(nrOfRecords / calcNrOfWorkers, 1);

      // Remaining jobs, that are not assigned to a job yet
      int remaining = nrOfRecords - (sizeOfChunks * calcNrOfWorkers);

      // How many of the remaining jobs will be assigned to one job
      int remainingPerJob = remaining > 0 ? Math.max(remaining / calcNrOfWorkers, 1) : 0;
      int to = 0;
      int additionalJobsCnt = 0;
      for (int i = 0; i < calcNrOfWorkers; i++) {
        int from = i == 0 ? 0 : to;
        to = from + sizeOfChunks + remainingPerJob;

        // Calculates the remaining jobs that will be assigned to the new FileJob.
        additionalJobsCnt += remainingPerJob;
        if (remainingPerJob != 0 && additionalJobsCnt > remaining) {
          remainingPerJob = additionalJobsCnt - remaining;
        } else if (additionalJobsCnt == remaining) {
          remainingPerJob = 0;
        }
        // Awaits for an available thread
        Lock lock = getLock();
        DownloadFileWork work =
            new DownloadFileWork(
                from,
                to,
                jobConfiguration.getSourceDir()
                    + Path.SEPARATOR
                    + jobConfiguration.getDownloadKey()
                    + Path.SEPARATOR
                    + jobConfiguration.getDownloadTableName(),
                i,
                jobConfiguration.getSearchQuery(),
                lock,
                esClient,
                esIndex,
                jobConfiguration.getVerbatimExtensions(),
                jobConfiguration.getInterpretedExtensions(),
                jobConfiguration.getDownloadFormat());

        log.info("Requesting a lock for job {}, detail: {}", i, work);
        lock.lock();
        log.info("Lock granted for job {}, detail: {}", i, work);
        // Submits the job to the pool. The file name is the output file name + the sequence i
        // A new worker is created per job to avoid sharing mutable writer state across concurrent jobs.
        DownloadFileWorker worker = createWorker();
        futures.add(executor.submit(() -> worker.work(work)));
      }

      List<Result> results = new ArrayList<>(futures.size());
      try {
        for (Future<Result> future : futures) {
          results.add(future.get());
        }
      } catch (ExecutionException | InterruptedException e) {
        log.error("Received an exception from a worker. Aborting.", e);
        executor.shutdownNow();
        throw new RuntimeException(e);
      }
      return results;
    } finally {
      executor.shutdown();
    }
  }

  /** Creates the worker to be used for the job's download format. */
  private DownloadFileWorker createWorker() {

    DownloadFormat downloadFormat = jobConfiguration.getDownloadFormat();
    SearchQueryProcessor<Occurrence, OccurrenceSearchParameter> queryProcessor =
        new SearchQueryProcessor<>(
            new OccurrenceEsResponseParser(occurrenceEsFieldMapper, searchHitConverter),
            sortField);

    return switch (downloadFormat) {
      case SIMPLE_CSV -> new SimpleCsvDownloadWorker<>(queryProcessor, interpretedMapper);
      case DWCA, FASTA_ARCHIVE ->
        new DwcaDownloadWorker<>(queryProcessor, verbatimMapper, interpretedMapper);
      case SPECIES_LIST -> new SpeciesListDownloadWorker<>(queryProcessor, interpretedMapper);
      default -> throw new IllegalStateException(
        "Download format '"
          + downloadFormat
          + "' unknown or not supported for small downloads.");
    };
  }

  /**
   * Utility class that holds the general execution settings.
   */
  @Data
  @Builder
  public static class MasterConfiguration {

    // Maximum number of workers
    private final int nrOfWorkers;

    // Minimum number of records per job
    private final int minNrOfRecords;

    // Limits the maximum number of records that can be processed.
    // This parameters avoids use this class to create file with a size beyond a maximum.
    private final int maximumNrOfRecords;

    // Occurrence download lock/counter name
    private final String lockName;
  }
}
