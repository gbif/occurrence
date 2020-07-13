package org.gbif.occurrence.download.file;

import lombok.Builder;
import lombok.Data;
import org.apache.curator.framework.CuratorFramework;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.dwca.DownloadDwcaActor;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvDownloadActor;
import org.gbif.occurrence.download.file.specieslist.SpeciesListDownloadActor;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.FileUtils;
import org.gbif.wrangler.lock.Lock;
import org.gbif.wrangler.lock.LockFactory;
import org.gbif.wrangler.lock.zookeeper.ZooKeeperLockFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import akka.routing.RoundRobinRouter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Actor that controls the multi-threaded creation of occurrence downloads.
 */
@Data
public class DownloadMaster extends UntypedActor {

  private static final String RUNNING_JOBS_LOCKING_PATH = "/runningJobs/";

  private static final Logger LOG = LoggerFactory.getLogger(DownloadMaster.class);
  private static final String FINISH_MSG_FMT = "Time elapsed %d minutes and %d seconds";
  private final RestHighLevelClient esClient;
  private final String esIndex;
  private final MasterConfiguration conf;
  private final CuratorFramework curatorFramework;
  private final LockFactory lockFactory;
  private final DownloadAggregator aggregator;
  private final DownloadJobConfiguration jobConfiguration;
  private List<Result> results = Lists.newArrayList();
  private int calcNrOfWorkers;
  private int nrOfResults;

  /**
   * Default constructor.
   */
  @Builder
  public DownloadMaster(
    WorkflowConfiguration workflowConfiguration,
    MasterConfiguration masterConfiguration,
    RestHighLevelClient esClient,
    String esIndex,
    DownloadJobConfiguration jobConfiguration,
    DownloadAggregator aggregator,
    int maxGlobalJobs) {
    conf = masterConfiguration;
    this.jobConfiguration = jobConfiguration;
    this.curatorFramework = DownloadWorkflowModule.curatorFramework(workflowConfiguration);
    this.lockFactory = new ZooKeeperLockFactory(curatorFramework,
                                                maxGlobalJobs,
                                                RUNNING_JOBS_LOCKING_PATH);
    this.esClient = esClient;
    this.esIndex = esIndex;
    this.aggregator = aggregator;

  }

  /**
   * Aggregates the result and shutdown the system of actors
   */
  private void aggregateAndShutdown() {
    aggregator.aggregate(results);
    shutDownEsClientSilently();
    curatorFramework.close();
    getContext().stop(getSelf());
  }

  /**
   * Shuts down the ElasticSearch client
   */
  private void shutDownEsClientSilently() {
    try {
      if(Objects.nonNull(esClient)) {
        esClient.close();
      }
    } catch (IOException ex) {
      LOG.error("Error shutting down Elasticsearch client", ex);
    }
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof Start) {
      runActors();
    } else if (message instanceof Result) {
      results.add((Result) message);
      nrOfResults += 1;
      if (nrOfResults == calcNrOfWorkers) {
        aggregateAndShutdown();
      }
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
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(0);
      if (!Strings.isNullOrEmpty(query)) {
        searchSourceBuilder.query(QueryBuilders.wrapperQuery(query));
      } else {
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
      }
      SearchResponse searchResponse = esClient.search(new SearchRequest().indices(esIndex).source(searchSourceBuilder), RequestOptions.DEFAULT);
      return searchResponse.getHits().getTotalHits();
    } catch (Exception e) {
      LOG.error("Error executing query", e);
      return 0L;
    }
  }

  /**
   * Run the list of jobs. The amount of records is assigned evenly among the worker threads.
   * If the amount of records is not divisible by the calcNrOfWorkers the remaining records are assigned "evenly" among
   * the first jobs.
   */
  private void runActors() {
    StopWatch stopwatch = new StopWatch();
    stopwatch.start();
    LOG.info("Acquiring Search Index Read Lock");
    File downloadTempDir = new File(jobConfiguration.getDownloadTempDir());
    if (downloadTempDir.exists()) {
      FileUtils.deleteDirectoryRecursively(downloadTempDir);
    }
    downloadTempDir.mkdirs();

    int recordCount = getSearchCount(jobConfiguration.getSearchQuery()).intValue();
    if (recordCount <= 0) { // no work to do: shutdown the system
      aggregateAndShutdown();
    } else  {
      int nrOfRecords = Math.min(recordCount, conf.maximumNrOfRecords);
      // Calculates the required workers.
      calcNrOfWorkers =
        conf.minNrOfRecords >= nrOfRecords ? 1 : Math.min(conf.nrOfWorkers, nrOfRecords / conf.minNrOfRecords);

      ActorRef workerRouter =
        getContext().actorOf(new Props(new DownloadActorsFactory(jobConfiguration.getDownloadFormat())).withRouter(new RoundRobinRouter(
          calcNrOfWorkers)), "downloadWorkerRouter");

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
        DownloadFileWork work = new DownloadFileWork(from,
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
                                                     esIndex);

        LOG.info("Requesting a lock for job {}, detail: {}", i, work.toString());
        lock.lock();
        LOG.info("Lock granted for job {}, detail: {}", i, work.toString());
        // Adds the Job to the list. The file name is the output file name + the sequence i
        workerRouter.tell(work, getSelf());
      }

      stopwatch.stop();
      long timeInSeconds = TimeUnit.MILLISECONDS.toSeconds(stopwatch.getTime());
      LOG.info(String.format(FINISH_MSG_FMT, TimeUnit.SECONDS.toMinutes(timeInSeconds), timeInSeconds % 60));
    }
  }

  /**
   * Used as a command to start this master actor.
   */
  public static class Start { }

  /**
   * Creates an instance of the download actor/job to be used.
   */
  private static class DownloadActorsFactory implements UntypedActorFactory {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final DownloadFormat downloadFormat;

    DownloadActorsFactory(DownloadFormat downloadFormat) {
      this.downloadFormat = downloadFormat;
    }

    @Override
    public Actor create() throws Exception {
      switch (downloadFormat) {
        case SIMPLE_CSV:
          return new SimpleCsvDownloadActor();

        case DWCA:
          return new DownloadDwcaActor();

        case SPECIES_LIST:
          return new SpeciesListDownloadActor();

        default:
          throw new IllegalStateException("Download format '"+downloadFormat+"' unknown or not supported for small downloads.");
      }
    }
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
