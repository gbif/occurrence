package org.gbif.occurrence.download.inject;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.DownloadMaster;
import org.gbif.occurrence.download.oozie.DownloadPrepareAction;
import org.gbif.wrangler.lock.LockFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.BeanFactoryAnnotationUtils;
import org.springframework.context.ApplicationContext;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

public class DownloadWorkflowModuleTestIT {

   private static TestingCluster curatorTestingCluster;
   private static EmbeddedElastic embeddedElastic;

   @BeforeAll
   public static void setup() throws Exception {
     curatorTestingCluster = new TestingCluster(1);
     curatorTestingCluster.start();
     embeddedElastic = EmbeddedElastic.builder()
       .withIndex("occurrence")
       .withElasticVersion(getEsVersion())
       .withSetting(
       PopularProperties.HTTP_PORT, getAvailablePort())
       .build();
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

  private static int getAvailablePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();

    return port;
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
    properties.put(DownloadWorkflowModule.DefaultSettings.ES_HOSTS_KEY, "http://localhost:" + embeddedElastic.getHttpPort());

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

    ApplicationContext applicationContext = DownloadWorkflowModule.buildAppContext(workflowConfiguration(downloadFormat),
                                                                                   downloadJobConfiguration);

    Assertions.assertNotNull(applicationContext);

    DownloadPrepareAction downloadPrepareAction = applicationContext.getBean(DownloadPrepareAction.class);
    Assertions.assertNotNull(downloadPrepareAction);

    ActorSystem system = ActorSystem.create("DownloadSystem" + downloadJobConfiguration.getDownloadKey());
    ActorRef downloadMaster = applicationContext.getBean(DownloadMaster.MasterFactory.class).build(system);
    Assertions.assertNotNull(downloadMaster);

    DownloadAggregator aggregator = applicationContext.getBean(DownloadAggregator.class);
    Assertions.assertNotNull(aggregator);

    LockFactory lockFactory = applicationContext.getBean(LockFactory.class);
    Assertions.assertNotNull(lockFactory);


    CuratorFramework curatorFrameworkDownloads = BeanFactoryAnnotationUtils.qualifiedBeanOfType(applicationContext.getAutowireCapableBeanFactory(), CuratorFramework.class, "Downloads");
    Assertions.assertNotNull(curatorFrameworkDownloads);

    CuratorFramework curatorFrameworkIndices = BeanFactoryAnnotationUtils.qualifiedBeanOfType(applicationContext.getAutowireCapableBeanFactory(), CuratorFramework.class, "Indices");
    Assertions.assertNotNull(curatorFrameworkIndices);
  }

  @Test
  public void DownloadPrepareActionCreationTest() {

    ApplicationContext applicationContext = DownloadWorkflowModule.buildAppContext(workflowConfiguration(DownloadFormat.DWCA));
    Assertions.assertNotNull(applicationContext);

    DownloadPrepareAction downloadPrepareAction = applicationContext.getBean(DownloadPrepareAction.class);
    Assertions.assertNotNull(downloadPrepareAction);

    Assertions.assertThrows(NoSuchBeanDefinitionException.class, () -> applicationContext.getBean(DownloadMaster.MasterFactory.class));
  }
}
