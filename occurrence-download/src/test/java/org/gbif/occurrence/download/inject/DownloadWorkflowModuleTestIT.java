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
package org.gbif.occurrence.download.inject;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.oozie.DownloadPrepareAction;

import java.io.IOException;
import java.util.Properties;

import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DownloadWorkflowModuleTestIT {

   private static TestingCluster curatorTestingCluster;
   private static ElasticsearchContainer embeddedElastic;

   @BeforeAll
   public static void setup() throws Exception {
     curatorTestingCluster = new TestingCluster(1);
     curatorTestingCluster.start();
     embeddedElastic =
       new ElasticsearchContainer(
         "docker.elastic.co/elasticsearch/elasticsearch:" + getEsVersion());
     embeddedElastic.start();
   }

   @AfterAll
   public static void tearDown() throws Exception {
     if (curatorTestingCluster != null) {
       curatorTestingCluster.stop();
     }
     if (embeddedElastic != null) {
       embeddedElastic.stop();
     }
   }

  private static String getEsVersion() throws IOException {
    Properties properties = new Properties();
    properties.load(DownloadWorkflowModuleTestIT.class.getClassLoader().getResourceAsStream("maven.properties"));
    return properties.getProperty("elasticsearch.version");
  }

  private static WorkflowConfiguration workflowConfiguration(DownloadFormat downloadFormat) {
    Properties properties = new Properties();
    properties.put(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY, "hdfs://ha-nn/");
    properties.put(DownloadWorkflowModule.DefaultSettings.HIVE_DB_KEY, "test");
    properties.put(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY, "http://localhost:8080");
    properties.put(DownloadWorkflowModule.DefaultSettings.API_URL_KEY, "http://localhost:8080");
    properties.put(DownloadWorkflowModule.DefaultSettings.ES_INDEX_KEY, "occurrence");
    properties.put(DownloadWorkflowModule.DefaultSettings.ES_HOSTS_KEY, "http://localhost:" + embeddedElastic.getMappedPort(9200));
    properties.put(DownloadWorkflowModule.DefaultSettings.ES_SNIFF_INTERVAL_KEY, "-1");

    properties.put(DownloadWorkflowModule.DefaultSettings.MAX_THREADS_KEY, "2");
    properties.put(DownloadWorkflowModule.DefaultSettings.MAX_GLOBAL_THREADS_KEY, "5");
    properties.put(DownloadWorkflowModule.DefaultSettings.JOB_MIN_RECORDS_KEY, "10");
    properties.put(DownloadWorkflowModule.DefaultSettings.MAX_RECORDS_KEY, "100");
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_LOCK_NAME_KEY, "testLock");

    properties.put(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_USER_KEY, "downloadUser");
    properties.put(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_PASSWORD_KEY, "downloadUserPassword");
    properties.put(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_LINK_KEY, "http://localhost:8080");
    properties.put(DownloadWorkflowModule.DefaultSettings.HDFS_OUTPUT_PATH_KEY, "/tmp/");
    properties.put(DownloadWorkflowModule.DefaultSettings.TMP_DIR_KEY, "/tmp/");
    properties.put(DownloadWorkflowModule.DefaultSettings.HIVE_DB_PATH_KEY, "/tmp/");

    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_INDICES_NS_KEY, "indices");
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_DOWNLOADS_NS_KEY, "downloads");
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_QUORUM_KEY, curatorTestingCluster.getConnectString());
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_SLEEP_TIME_KEY, "60000");
    properties.put(DownloadWorkflowModule.DefaultSettings.ZK_MAX_RETRIES_KEY, "1");

    properties.put(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_FORMAT_KEY, downloadFormat.name());

    return new WorkflowConfiguration(properties);
  }

  private DownloadJobConfiguration downloadJobConfiguration(DownloadFormat downloadFormat) {
     return new DownloadJobConfiguration.Builder()
                 .withDownloadFormat(downloadFormat)
                 .withDownloadKey("1")
                 .withDownloadTableName("occurrence")
                 .withIsSmallDownload(true)
                 .withFilter("*")
                 .withSourceDir("/tmp/")
                 .withUser("testUser")
                 .withSearchQuery("*").build();
  }

  @ParameterizedTest
  @EnumSource(value = DownloadFormat.class)
  public void downloadModuleCreationTest(DownloadFormat downloadFormat) {
     DownloadJobConfiguration downloadJobConfiguration =downloadJobConfiguration(downloadFormat);

    DownloadWorkflowModule module = DownloadWorkflowModule.builder()
                                      .workflowConfiguration(workflowConfiguration(downloadFormat))
                                      .downloadJobConfiguration(downloadJobConfiguration).build();

    assertNotNull(module);

    DownloadPrepareAction downloadPrepareAction = module.downloadPrepareAction();
    assertNotNull(downloadPrepareAction);


    ActorSystem system = ActorSystem.create("DownloadSystem" + downloadJobConfiguration.getDownloadKey());

    ActorRef downloadMaster = module.downloadMaster(system);
    assertNotNull(downloadMaster);
  }
}
