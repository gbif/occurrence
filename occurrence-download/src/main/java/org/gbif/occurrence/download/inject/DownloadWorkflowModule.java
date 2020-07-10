package org.gbif.occurrence.download.inject;

import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.DownloadMaster;
import org.gbif.occurrence.download.file.dwca.DwcaDownloadAggregator;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvDownloadAggregator;
import org.gbif.occurrence.download.file.specieslist.SpeciesListDownloadAggregator;
import org.gbif.occurrence.download.oozie.DownloadPrepareAction;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.wrangler.lock.LockFactory;
import org.gbif.wrangler.lock.Mutex;
import org.gbif.wrangler.lock.ReadWriteMutexFactory;
import org.gbif.wrangler.lock.zookeeper.ZooKeeperLockFactory;
import org.gbif.wrangler.lock.zookeeper.ZookeeperSharedReadWriteMutex;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import akka.dispatch.ExecutionContextExecutorService;
import akka.dispatch.ExecutionContexts;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.MapPropertySource;

/**
 * Private guice module that provides bindings the required Modules and dependencies.
 * The following class are exposed:
 * - CuratorFramework: this class is exposed only to close the zookeeper connections properly.
 * - OccurrenceFileWriter: class that creates the occurrence data and citations file.
 */
@Configuration
public class DownloadWorkflowModule  {

  public static final String CONF_FILE = "occurrence-download.properties";

  //Prefix for static settings
  public static final String PROPERTIES_PREFIX = "occurrence.download.";
  private static final String ES_PREFIX = "es.";

  private static final String RUNNING_JOBS_LOCKING_PATH = "/runningJobs/";

  private static final String INDEX_LOCKING_PATH = "/indices/";


  public static ApplicationContext buildAppContext(WorkflowConfiguration workflowConfiguration) {
    return buildAppContext(workflowConfiguration, null);
  }


  public static ApplicationContext buildAppContext(WorkflowConfiguration workflowConfiguration, DownloadJobConfiguration configuration) {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();

    ctx.register(DownloadWorkflowModule.class);

    ctx.registerBean(
      "workflowConfiguration",
      WorkflowConfiguration.class,
      () -> workflowConfiguration);

    if (configuration != null) {
      ctx.registerBean("jobConfiguration", DownloadJobConfiguration.class, () -> configuration);
      ctx.register(DownloadMaster.MasterConfiguration.class);
      registerAggregator(workflowConfiguration, ctx);
    }

    ctx.register(DownloadPrepareAction.class);

    ctx.getEnvironment()
      .getPropertySources()
      .addLast(
        new MapPropertySource(
          "ManagedProperties", workflowConfiguration.getDownloadSettings()
                                        .entrySet()
                                        .stream()
                                        .collect(Collectors.toMap( e -> e.getKey().toString(), e -> e.getValue().toString()))));
    ctx.refresh();
    ctx.start();
    return ctx;
  }

  @Bean
  public RegistryClientUtil registryClientUtil(@Value("${" + DownloadWorkflowModule.DefaultSettings.DOWNLOAD_USER_KEY + "}") String userName,
                                               @Value("${" + DownloadWorkflowModule.DefaultSettings.DOWNLOAD_PASSWORD_KEY + "}") String password,
                                               @Value("${" + DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY + "}") String apiUrl) {
    return new RegistryClientUtil(userName, password, apiUrl);
  }

  @Bean
  @Qualifier("Downloads")
  @ConditionalOnBean(DownloadJobConfiguration.class)
  CuratorFramework provideCuratorFrameworkDownloads(@Value("${" + DefaultSettings.ZK_DOWNLOADS_NS_KEY +"}") String zookeeperNamespace,
                                                    @Value("${" + DefaultSettings.ZK_QUORUM_KEY + "}") String zookeeperConnection,
                                                    @Value("${" + DefaultSettings.ZK_SLEEP_TIME_KEY + "}") Integer sleepTime,
                                                    @Value("${" + DefaultSettings.ZK_MAX_RETRIES_KEY + "}") Integer maxRetries) {
    CuratorFramework curator = CuratorFrameworkFactory.builder().namespace(zookeeperNamespace)
      .retryPolicy(new ExponentialBackoffRetry(sleepTime, maxRetries))
      .connectString(zookeeperConnection)
      .build();
    curator.start();
    return curator;
  }

  @Bean
  @Qualifier("Indices")
  @ConditionalOnBean(DownloadJobConfiguration.class)
  CuratorFramework provideCuratorFrameworkIndices(@Value("${" + DefaultSettings.ZK_INDICES_NS_KEY +"}") String zookeeperNamespace,
                                                  @Value("${" + DefaultSettings.ZK_QUORUM_KEY + "}") String zookeeperConnection,
                                                  @Value("${" + DefaultSettings.ZK_SLEEP_TIME_KEY + "}") Integer sleepTime,
                                                  @Value("${" + DefaultSettings.ZK_MAX_RETRIES_KEY + "}") Integer maxRetries) {
    CuratorFramework curator = CuratorFrameworkFactory.builder().namespace(zookeeperNamespace)
      .retryPolicy(new ExponentialBackoffRetry(sleepTime, maxRetries))
      .connectString(zookeeperConnection)
      .build();
    curator.start();
    return curator;
  }

  @Bean
  @ConditionalOnBean(DownloadJobConfiguration.class)
  DatasetOccurrenceDownloadUsageService provideDatasetOccurrenceDownloadUsageService(RegistryClientUtil registryClientUtil) {
    return registryClientUtil.setupDatasetUsageService();
  }

  @Bean
  @ConditionalOnBean(DownloadJobConfiguration.class)
  DatasetService provideDatasetService(RegistryClientUtil registryClientUtil) {
    return registryClientUtil.setupDatasetService();
  }

  @Bean
  OccurrenceDownloadService provideOccurrenceDownloadService(RegistryClientUtil registryClientUtil) {
    return registryClientUtil.setupOccurrenceDownloadService();
  }

  @Bean
  @ConditionalOnBean(DownloadJobConfiguration.class)
  ExecutionContextExecutorService provideExecutionContextExecutorService(
    @Value("${" + PROPERTIES_PREFIX + "job.max_threads}") int maxThreads) {
    return ExecutionContexts.fromExecutorService(Executors.newFixedThreadPool(maxThreads));
  }

  @Bean
  @ConditionalOnBean(DownloadJobConfiguration.class)
  LockFactory provideLock(@Qualifier("Downloads") CuratorFramework curatorFramework, @Value("${" +  DefaultSettings.MAX_GLOBAL_THREADS_KEY + "}") Integer maxGlobalThreads) {
    return new ZooKeeperLockFactory(curatorFramework, maxGlobalThreads, RUNNING_JOBS_LOCKING_PATH);
  }

  @Bean
  @ConditionalOnBean(DownloadJobConfiguration.class)
  ReadWriteMutexFactory provideMutexFactory(@Qualifier("Indices") CuratorFramework curatorFramework) {
    return new ZookeeperSharedReadWriteMutex(curatorFramework, INDEX_LOCKING_PATH);
  }

  @Bean
  @ConditionalOnBean(DownloadJobConfiguration.class)
  Mutex provideReadLock(ReadWriteMutexFactory readWriteMutexFactory,  @Value("${" + DownloadWorkflowModule.DefaultSettings.ES_INDEX_KEY + "}") String esIndex) {
    return readWriteMutexFactory.createReadMutex(esIndex);
  }

  @Bean
  RestHighLevelClient provideEsClient(WorkflowConfiguration workflowConfiguration) {
    EsConfig esConfig = EsConfig.fromProperties(workflowConfiguration.getDownloadSettings(), ES_PREFIX);
    HttpHost[] hosts = new HttpHost[esConfig.getHosts().length];
    int i = 0;
    for (String host : esConfig.getHosts()) {
      try {
        URL url = new URL(host);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }

    SniffOnFailureListener sniffOnFailureListener =
      new SniffOnFailureListener();

    RestClientBuilder builder =
      RestClient.builder(hosts)
        .setRequestConfigCallback(
          requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(esConfig.getConnectTimeout())
            .setSocketTimeout(esConfig.getSocketTimeout()))
        .setMaxRetryTimeoutMillis(esConfig.getSocketTimeout())
        .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS)
        .setFailureListener(sniffOnFailureListener);

    RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);

    Sniffer sniffer =
      Sniffer.builder(highLevelClient.getLowLevelClient())
        .setSniffIntervalMillis(esConfig.getSniffInterval())
        .setSniffAfterFailureDelayMillis(esConfig.getSniffAfterFailureDelay())
        .build();
    sniffOnFailureListener.setSniffer(sniffer);

    Runtime.getRuntime()
      .addShutdownHook(
        new Thread(
          () -> {
            sniffer.close();
            try {
              highLevelClient.close();
            } catch (IOException e) {
              throw new IllegalStateException("Couldn't close ES client", e);
            }
          }));

    return highLevelClient;
  }

  @Bean
  @ConditionalOnBean(DownloadJobConfiguration.class)
  DownloadMaster.MasterFactory downloadMaster(LockFactory lockFactory,
                                DownloadMaster.MasterConfiguration masterConfiguration,
                                RestHighLevelClient esClient,
                                @Value("${" + DownloadWorkflowModule.DefaultSettings.ES_INDEX_KEY+ "}") String esIndex,
                                DownloadJobConfiguration jobConfiguration,
                                DownloadAggregator aggregator) {
    return new DownloadMaster.MasterFactory(lockFactory, masterConfiguration, esClient, esIndex, jobConfiguration, aggregator);
  }

  /**
   * Binds a DownloadFilesAggregator according to the DownloadFormat set using the key DOWNLOAD_FORMAT_KEY.
   */

  private static void registerAggregator(WorkflowConfiguration workflowConfiguration, AnnotationConfigApplicationContext context) {
    DownloadFormat downloadFormat = workflowConfiguration.getDownloadFormat();

    if (downloadFormat != null) {
      switch (downloadFormat) {
        case DWCA:
          context.registerBean(DwcaDownloadAggregator.class);
          break;

        case SIMPLE_CSV:
          context.registerBean(SimpleCsvDownloadAggregator.class);
          break;

        case SPECIES_LIST:
          context.registerBean(SpeciesListDownloadAggregator.class);
          break;

        case SIMPLE_AVRO:
        case SIMPLE_WITH_VERBATIM_AVRO:
        case IUCN:
        case MAP_OF_LIFE:
        case BLOODHOUND:
          context.registerBean(NotSupportedDownloadAggregator.class);
          break;

        default:
          throw new IllegalStateException("Unknown download format '" + downloadFormat + "'.");
      }
    }
  }

  /**
   * Utility class that contains constants and keys set each time the workflow is executed.
   */
   public static final class DynamicSettings {

    /**
     * Hidden constructor.
     */
    private DynamicSettings() {
      //empty
    }

    //Prefix for dynamic settings usually set each time that workflow is executed
    public static final String WORKFLOW_PROPERTIES_PREFIX = PROPERTIES_PREFIX + "workflow.";

    public static final String DOWNLOAD_FORMAT_KEY = WORKFLOW_PROPERTIES_PREFIX + "format";

  }

  /**
   * Utility class that contains configuration keys of common settings.
   */
  public static final class DefaultSettings {

    public static final String NAME_NODE_KEY = "hdfs.namenode";
    public static final String HIVE_DB_KEY = "hive.db";
    public static final String REGISTRY_URL_KEY = "registry.ws.url";
    public static final String API_URL_KEY = "api.url";
    public static final String ES_INDEX_KEY = "es.index";
    public static final String ES_HOSTS_KEY = "es.hosts";

    /**
     * Hidden constructor.
     */
    private DefaultSettings() {
      //empty
    }
    public static final String MAX_THREADS_KEY = PROPERTIES_PREFIX + "job.max_threads";
    public static final String JOB_MIN_RECORDS_KEY = PROPERTIES_PREFIX + "job.min_records";
    public static final String MAX_RECORDS_KEY = PROPERTIES_PREFIX + "file.max_records";
    public static final String ZK_LOCK_NAME_KEY = PROPERTIES_PREFIX + "zookeeper.lock_name";
    public static final String MAX_GLOBAL_THREADS_KEY = PROPERTIES_PREFIX + "max_global_threads";

    public static final String DOWNLOAD_USER_KEY = PROPERTIES_PREFIX + "ws.username";
    public static final String DOWNLOAD_PASSWORD_KEY = PROPERTIES_PREFIX + "ws.password";
    public static final String DOWNLOAD_LINK_KEY = PROPERTIES_PREFIX + "link";
    public static final String HDFS_OUTPUT_PATH_KEY = PROPERTIES_PREFIX + "hdfsOutputPath";
    public static final String TMP_DIR_KEY = PROPERTIES_PREFIX + "tmp.dir";
    public static final String HIVE_DB_PATH_KEY = PROPERTIES_PREFIX + "hive.hdfs.out";


    public static final String ZK_INDICES_NS_KEY = PROPERTIES_PREFIX + "zookeeper.indices.namespace";
    public static final String ZK_DOWNLOADS_NS_KEY = PROPERTIES_PREFIX + "zookeeper.downloads.namespace";
    public static final String ZK_QUORUM_KEY = PROPERTIES_PREFIX + "zookeeper.quorum";
    public static final String ZK_SLEEP_TIME_KEY = PROPERTIES_PREFIX + "zookeeper.sleep_time";
    public static final String ZK_MAX_RETRIES_KEY = PROPERTIES_PREFIX + "zookeeper.max_retries";

  }

}
