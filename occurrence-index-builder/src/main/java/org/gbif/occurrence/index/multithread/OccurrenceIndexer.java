package org.gbif.occurrence.index.multithread;

import org.gbif.occurrence.index.hadoop.EmbeddedSolrServerBuilder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import akka.dispatch.Await;
import akka.dispatch.ExecutionContextExecutorService;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import akka.util.Timeout;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that creates a single CSV file using a OccurrenceSearchRequest.
 * The file is built with a set of worker threads that create a piece of the output file, once the jobs have finished
 * their jobs a single file is created. The order of the records is respected from the search request by using the
 * fileJob.startOffset reported by each job.
 */
public class OccurrenceIndexer {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceIndexer.class);

  // Awaiting time in minutes. This is the maximum time that is given to job to finish its work.
  private static final int AWAIT_TIME = 60;

  private static final String FINISH_MSG_FMT = "Time elapsed %d  minutes and %d seconds";

  private final SolrServer solrServer;

  // Maximum number of workers
  private final int nrOfWorkers;

  // Number of records
  private final long nrOfRecords;

  private final String hTable;

  /**
   * Default constructor.
   */
  @Inject
  public OccurrenceIndexer(String solrHome, String hTable, int nrOfWorkers, long nrOfRecords) {
    this.nrOfRecords = nrOfRecords;
    this.nrOfWorkers = nrOfWorkers;
    this.solrServer = EmbeddedSolrServerBuilder.build(solrHome);
    this.hTable = hTable;
  }

  public static void main(String[] args) {
    OccurrenceIndexer writer =
      new OccurrenceIndexer(args[0], args[1], Integer.parseInt(args[3]), Long.parseLong(args[4]));
    writer.run();
  }

  /**
   * Entry point. This method: creates the output files, runs the jobs (OccurrenceFileWriterJob) and then collect the
   * results. Individual files created by each job are deleted.
   */
  public void run() {
    try {
      StopWatch stopwatch = new StopWatch();
      stopwatch.start();
      ExecutionContextExecutorService executionContext =
        ExecutionContexts.fromExecutorService(Executors.newFixedThreadPool(nrOfWorkers));
      collectResults(Futures.sequence(runHBaseJobs(executionContext), executionContext));
      stopwatch.stop();
      executionContext.shutdownNow();
      commitQuietly(solrServer);
      shutdownSolrServer(solrServer);
      final long timeInSeconds = TimeUnit.MILLISECONDS.toSeconds(stopwatch.getTime());
      System.out.println(String.format(FINISH_MSG_FMT, TimeUnit.SECONDS.toMinutes(timeInSeconds), timeInSeconds % 60));
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  private void collectResults(Future<Iterable<Integer>> futures) throws IOException {
    Closer outFileCloser = Closer.create();
    try {
      List<Integer> results =
        Lists.newArrayList(Await.result(futures, new Timeout(AWAIT_TIME, TimeUnit.MINUTES).duration()));
      Integer total = 0;
      for (Integer result : results) {
        total += result;
      }
      System.out.println(total + " documents indexed");
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
   * Commits changes to the solr server.
   * Exceptions {@link SolrServerException} and {@link IOException} are swallowed.
   */
  private void commitQuietly(SolrServer solrServer) {
    try {
      solrServer.commit();
    } catch (SolrServerException e) {
      LOG.error("Solr error commiting on Solr server", e);
    } catch (IOException e) {
      LOG.error("I/O error commiting on Solr server", e);
    }
  }


  /**
   * Run the list of jobs. The amount of records is assigned evenly among the worker threads.
   * If the amount of records is not divisible by the nrOfWorkers the remaining records are assigned "evenly" among the
   * first jobs.
   */
  private List<Future<Integer>> runHBaseJobs(ExecutionContextExecutorService executionContext) throws IOException {

    HTable table = null;
    ResultScanner scanner = null;
    // Number of records that will be assigned to each job
    final long sizeOfChunks = Math.max(nrOfRecords / nrOfWorkers, 1);

    // Remaining jobs, that are not assigned to a job yet
    final long remaining = nrOfRecords - (sizeOfChunks * nrOfWorkers);

    // How many of the remaining jobs will be assigned to one job
    long remainingPerJob = remaining > 0 ? Math.max(remaining / nrOfWorkers, 1) : 0;

    List<Future<Integer>> futures = Lists.newArrayList();
    int numDocs = 0;
    // The doc instance is re-used

    Closer closer = Closer.create();
    try {
      Configuration conf = HBaseConfiguration.create();
      table = new HTable(conf, hTable);
      closer.register(table);
      Scan scan = new Scan();
      scan.setCaching(200);
      scanner = table.getScanner(scan);
      closer.register(scanner);
      long from;
      long to = 0;
      long additionalJobsCnt = 0;
      for (int i = 0; i < nrOfWorkers; i++) {
        from = i == 0 ? 0 : to;
        to = from + sizeOfChunks + remainingPerJob;

        // Calculates the remaining jobs that will be assigned to the new FileJob.
        additionalJobsCnt += remainingPerJob;
        if (remainingPerJob != 0 && additionalJobsCnt > remaining) {
          remainingPerJob = additionalJobsCnt - remaining;
        } else if (additionalJobsCnt == remaining) {
          remainingPerJob = 0;
        }
        Result startRow = scanner.next();
        if (startRow != null) {
          Result stopRow = startRow;
          long iJob = 0;
          for (iJob = from + 1; stopRow != null && iJob <= to; iJob++) {
            stopRow = scanner.next();
          }
          System.out.println(iJob);
          System.out.println("Assiging occ records from " + from + " to " + to);
          futures
            .add(Futures.future(
              new OccurrenceIndexingJob(Bytes.toInt(startRow.getRow()), Bytes.toInt(stopRow.getRow()), solrServer,
                hTable), executionContext));
        }
        numDocs++;
      }
    } catch (Exception ex) {
      LOG.error("A general error has occurred", ex);
    } finally {
      closer.close();
      LOG.info("Commiting {} documents", numDocs);
      LOG.info("Indexing job has finished, # of documents indexed: {}", numDocs);
    }
    return futures;
  }

  /**
   * Shutdowns the Solr server.
   * This methods works only if server is an {@link EmbeddedSolrServer}.
   */
  private void shutdownSolrServer(SolrServer solrServer) {
    if (solrServer instanceof EmbeddedSolrServer) {
      ((EmbeddedSolrServer) solrServer).shutdown();
    }
  }
}
