package org.gbif.occurrence.download.file;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.wrangler.lock.Lock;

import java.io.IOException;
import java.util.concurrent.Callable;

import akka.dispatch.Future;
import org.apache.solr.client.solrj.SolrServer;

public interface DownloadFilesAggregator {

  void init(String baseTableName, DownloadFormat downloadFormat);

  void aggregateResults(Future<Iterable<Result>> futures, String baseTableName) throws IOException;

  Callable<Result> createJob(FileJob fileJob, Lock lock, SolrServer solrServer, OccurrenceMapReader occurrenceMapReader);
}
