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
import org.gbif.api.vocabulary.Extension;
import org.gbif.wrangler.lock.Lock;

import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.client.RestHighLevelClient;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import static org.gbif.occurrence.download.util.Preconditions.checkArgument;

/**
 * Holds the job information about the work that each file writer job/thread has to do.
 * Examples of instances of this class are:
 * - query:*:* from: 200, to: 500, dataFile:occurrence.txt3.
 * - query:collector_name:juan from: 1000, to: 5500, dataFile:occurrence.txt99.
 */
@Getter
@EqualsAndHashCode(of = {"baseDataFileName", "jobId", "query", "from", "to"})
@ToString(of = {"from", "to", "baseDataFileName", "jobId", "query"})
public class DownloadFileWork implements Comparable<DownloadFileWork> {

  /**
   * -- GETTER --
   *  Search query.
   *
   * @return the query
   */
  private final String query;

  /**
   * -- GETTER --
   *  Offset in the complete result set returned by the query.
   *
   * @return the from
   */
  private final int from;

  /**
   * -- GETTER --
   *  Number of the last result to be processed.
   *
   * @return the to
   */
  private final int to;

  /**
   * -- GETTER --
   *
   * @return the job identifier
   */
  private final int jobId;

  /**
   * -- GETTER --
   *  Gets the base table name.
   */
  private final String baseDataFileName;

  /**
   * -- GETTER --
   *
   * @return zookeeper lock
   */
  private final Lock lock;

  /**
   * -- GETTER --
   *
   * @return Elasticsearch client to run queries
   */
  private final RestHighLevelClient esClient;

  /**
   * -- GETTER --
   *
   * @return the Elasticsearch index
   */
  private final String esIndex;

  private final Set<Extension> verbatimExtensions;

  private final Set<Extension> interpretedExtensions;

  private final DownloadFormat downloadFormat;


  /**
   * Default constructor.
   */
  public DownloadFileWork(int from, int to, String baseDataFileName, int jobId, String query, Lock lock,
                          RestHighLevelClient esClient, String esIndex, Set<Extension> verbatimExtensions,
                          Set<Extension> interpretedExtensions, DownloadFormat downloadFormat) {
    checkArgument(to >= from, "'to' parameter should be greater than the 'from' argument");
    this.query = query;
    this.from = from;
    this.to = to;
    this.baseDataFileName = baseDataFileName;
    this.jobId = jobId;
    this.lock = lock;
    this.esClient = esClient;
    this.esIndex = esIndex;
    this.verbatimExtensions = verbatimExtensions != null ? verbatimExtensions : new HashSet<>();
    this.interpretedExtensions = interpretedExtensions != null ? interpretedExtensions : new HashSet<>();
    this.downloadFormat = downloadFormat;
  }

  /**
   * Instance are compared by its job.from field.
   */
  @Override
  public int compareTo(DownloadFileWork that) {
    return Integer.compare(getFrom(), that.getFrom());
  }

  /**
   * @return baseDataFileName + jobId.
   */
  public String getJobDataFileName() {
    return baseDataFileName + jobId;
  }

}
