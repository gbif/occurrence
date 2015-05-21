package org.gbif.occurrence.download.file;

import org.gbif.wrangler.lock.Lock;

import com.google.common.base.Objects;
import com.google.common.primitives.Ints;
import org.apache.solr.client.solrj.SolrServer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Holds the job information about the work that each file writer job/thread has to do.
 * Examples of instances of this class are:
 * - query:*:* from: 200, to: 500, dataFile:occurrence.txt3.
 * - query:collector_name:juan from: 1000, to: 5500, dataFile:occurrence.txt99.
 */
public class DownloadFileWork implements Comparable<DownloadFileWork> {

  private final String query;

  private final int from;

  private final int to;

  private final int jobId;

  private final String baseDataFileName;

  private final Lock lock;

  private final SolrServer solrServer;

  private final OccurrenceMapReader occurrenceMapReader;

  /**
   * Default constructor.
   */
  public DownloadFileWork(
    int from,
    int to,
    String baseDataFileName,
    int jobId,
    String query,
    Lock lock,
    SolrServer solrServer,
    OccurrenceMapReader occurrenceMapReader
  ) {
    checkArgument(to >= from, "'to' parameter should be greater than the 'from' argument");
    this.query = query;
    this.from = from;
    this.to = to;
    this.baseDataFileName = baseDataFileName;
    this.jobId = jobId;
    this.lock = lock;
    this.solrServer = solrServer;
    this.occurrenceMapReader = occurrenceMapReader;
  }

  /**
   * Instance are compared by its job.from field.
   */
  @Override
  public int compareTo(DownloadFileWork that) {
    return Ints.compare(getFrom(), that.getFrom());
  }

  /**
   * Gets the base table name.
   */
  public String getBaseDataFileName() {
    return baseDataFileName;
  }

  /**
   * @return baseDataFileName + jobId.
   */
  public String getJobDataFileName() {
    return baseDataFileName + jobId;
  }

  /**
   * Offset in the complete result set returned by the query.
   *
   * @return the from
   */
  public int getFrom() {
    return from;
  }

  /**
   * Search query.
   *
   * @return the query
   */
  public String getQuery() {
    return query;
  }

  /**
   * Number of the last result to be processed.
   *
   * @return the to
   */
  public int getTo() {
    return to;
  }

  /**
   * @return the job identifier
   */
  public int getJobId() {
    return jobId;
  }

  /**
   * @return zookeeper lock
   */
  public Lock getLock() {
    return lock;
  }

  /**
   * @return Solr server to run queries
   */
  public SolrServer getSolrServer() {
    return solrServer;
  }

  /**
   * @return reads Hbase results into HashMaps
   */
  public OccurrenceMapReader getOccurrenceMapReader() {
    return occurrenceMapReader;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(baseDataFileName, jobId, query, from, to);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof DownloadFileWork)) {
      return false;
    }

    DownloadFileWork that = (DownloadFileWork) obj;
    return Objects.equal(this.baseDataFileName, that.baseDataFileName)
           && Objects.equal(this.jobId, that.jobId)
           && Objects.equal(this.query, that.query)
           && Objects.equal(this.from, that.from)
           && Objects.equal(this.to, that.to);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("from", from)
      .add("to", to)
      .add("baseDataFileName", baseDataFileName)
      .add("jobId", jobId)
      .add("query", query)
      .toString();
  }

}
