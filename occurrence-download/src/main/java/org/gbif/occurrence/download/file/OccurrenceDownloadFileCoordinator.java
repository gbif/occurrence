package org.gbif.occurrence.download.file;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.wrangler.lock.Lock;

import java.io.IOException;
import java.util.concurrent.Callable;

import akka.dispatch.Future;
import org.apache.solr.client.solrj.SolrServer;

/**
 * Coordinates the process of building Jobs and collect the result from those Jobs.
 */
public interface OccurrenceDownloadFileCoordinator {

  /**
   * Initialize the process, this method is invoked before distributing the work among the jobs.
   * @param baseDataFileName output data file, it's a base name if multiple file are produced
   * @param downloadFormat requested download format
   * @param filter predicate filter
   */
  void init(OccurrenceDownloadConfiguration configuration);

  /**
   * Collects/aggregates the results produced.
   * @param futures list of future results produced by worker threads
   * @throws IOException in case files results can't be read/written
   */
  void aggregateResults(Future<Iterable<Result>> futures) throws Exception;

  /**
   * Factory method that creates a Job that reads data from Solr/Hbase and returns the result as a Future.
   * @param fileJob describes the work that has to be done by the new job
   * @param lock ZK lock used to controlled the maximum number of jobs available
   * @param solrServer from where the data is loaded
   * @param occurrenceMapReader used to load the details of occurrence records
   * @return
   */
  Callable<Result> createJob(FileJob fileJob, Lock lock, SolrServer solrServer, OccurrenceMapReader occurrenceMapReader);
}
