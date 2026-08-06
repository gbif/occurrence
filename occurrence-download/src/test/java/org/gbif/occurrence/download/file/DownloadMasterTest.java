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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import org.apache.curator.test.TestingCluster;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.search.es.SearchHitConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Covers the orchestration behavior: with no records there's nothing to run and the (empty)
 * result set is still aggregated, and a worker failure aborts the job (no aggregation) instead
 * of silently completing.
 */
class DownloadMasterTest {

  private static TestingCluster zkTestingCluster;

  @BeforeAll
  static void startZk() throws Exception {
    zkTestingCluster = new TestingCluster(1);
    zkTestingCluster.start();
  }

  @AfterAll
  static void stopZk() throws Exception {
    zkTestingCluster.stop();
  }

  private static WorkflowConfiguration workflowConfiguration() {
    Properties properties = new Properties();
    properties.put(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY, "hdfs://ha-nn/");
    properties.put(DownloadWorkflowModule.DefaultSettings.MAX_THREADS_KEY, "2");
    properties.put(DownloadWorkflowModule.DefaultSettings.MAX_GLOBAL_THREADS_KEY, "5");
    properties.put(DownloadWorkflowModule.DefaultSettings.JOB_MIN_RECORDS_KEY, "10");
    properties.put(DownloadWorkflowModule.DefaultSettings.MAX_RECORDS_KEY, "100");
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_LOCK_NAME_KEY, "testLock");
    properties.put(DownloadWorkflowModule.DefaultSettings.ES_REQUEST_BUFFER_LIMIT, "209715200");
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_INDICES_NS_KEY, "indices");
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_DOWNLOADS_NS_KEY, "downloads");
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_QUORUM_KEY, zkTestingCluster.getConnectString());
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_SLEEP_TIME_KEY, "1000");
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_MAX_RETRIES_KEY, "1");
    properties.put(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_FORMAT_KEY, DownloadFormat.SIMPLE_CSV.name());
    return new WorkflowConfiguration(properties);
  }

  private static DownloadJobConfiguration downloadJobConfiguration(String downloadKey) {
    return DownloadJobConfiguration.builder()
        .downloadFormat(DownloadFormat.SIMPLE_CSV)
        .downloadKey(downloadKey)
        .downloadTableName("occurrence")
        .isSmallDownload(true)
        .filter("*")
        .sourceDir(System.getProperty("java.io.tmpdir") + "/download-master-test")
        .user("testUser")
        .searchQuery("*")
        .verbatimExtensions(Collections.emptySet())
        .interpretedExtensions(Collections.emptySet())
        .build();
  }

  private static DownloadMaster.MasterConfiguration masterConfiguration() {
    return DownloadMaster.MasterConfiguration.builder()
        .nrOfWorkers(2)
        .minNrOfRecords(10)
        .maximumNrOfRecords(100)
        .lockName("testLock")
        .build();
  }

  private static SearchResponse searchResponseWithTotalHits(long total) {
    SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(total, TotalHits.Relation.EQUAL_TO), 0f);
    SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, false, null, 0);
    return new SearchResponse(sections, null, 1, 1, 0, 0L, new ShardSearchFailure[0], SearchResponse.Clusters.EMPTY);
  }

  private static DownloadMaster.DownloadMasterBuilder masterBuilder(
      RestHighLevelClient esClient, DownloadAggregator aggregator, String downloadKey) {
    return DownloadMaster.builder()
        .workflowConfiguration(workflowConfiguration())
        .masterConfiguration(masterConfiguration())
        .esClient(esClient)
        .esIndex("occurrence")
        .jobConfiguration(downloadJobConfiguration(downloadKey))
        .aggregator(aggregator)
        .maxGlobalJobs(5)
        .interpretedMapper(o -> Collections.emptyMap())
        .verbatimMapper(o -> Collections.emptyMap())
        .searchHitConverter(mock(SearchHitConverter.class));
  }

  @Test
  void noRecordsAggregatesEmptyResults() throws Exception {
    RestHighLevelClient esClient = mock(RestHighLevelClient.class);
    when(esClient.search(any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(searchResponseWithTotalHits(0));

    DownloadAggregator aggregator = mock(DownloadAggregator.class);

    DownloadMaster master = masterBuilder(esClient, aggregator, "no-records").build();

    master.run();

    verify(aggregator).aggregate(Collections.emptyList());
    verify(esClient).close();
  }

  @Test
  void workerFailureAbortsWithoutAggregating() throws Exception {
    RestHighLevelClient esClient = mock(RestHighLevelClient.class);
    when(esClient.search(any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(searchResponseWithTotalHits(5))
        .thenThrow(new IOException("Simulated ES failure"));

    DownloadAggregator aggregator = mock(DownloadAggregator.class);

    DownloadMaster master = masterBuilder(esClient, aggregator, "worker-failure").build();

    assertThrows(RuntimeException.class, master::run);

    verify(aggregator, never()).aggregate(any());
    verify(esClient).close();
  }
}
