package org.gbif.occurrence.download.inject;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.common.search.solr.SolrConfig;
import org.gbif.common.search.solr.SolrModule;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.OccurrenceMapReader;
import org.gbif.occurrence.download.file.dwca.DwcaDownloadAggregator;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvDownloadAggregator;
import org.gbif.occurrence.download.oozie.DownloadPrepareAction;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.wrangler.lock.LockFactory;
import org.gbif.wrangler.lock.zookeeper.ZooKeeperLockFactory;

import java.io.IOException;
import java.util.concurrent.Executors;

import akka.dispatch.ExecutionContextExecutorService;
import akka.dispatch.ExecutionContexts;
import com.google.common.base.Optional;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Private guice module that provides bindings the required Modules and dependencies.
 * The following class are exposed:
 * - CuratorFramework: this class is exposed only to close the zookeeper connections properly.
 * - OccurrenceFileWriter: class that creates the occurrence data and citations file.
 */
public final class DownloadWorkflowModule extends AbstractModule {

  public static final String CONF_FILE = "occurrence-download.properties";

  //Prefix for static settings
  public static final String PROPERTIES_PREFIX = "occurrence.download.";
  private static final String PROPERTIES_SOLR_PREFIX = PROPERTIES_PREFIX + "solr.";

  private static final String LOCKING_PATH = "/runningJobs/";

  private final Optional<DownloadJobConfiguration> configuration;

  private final WorkflowConfiguration workflowConfiguration;

  /**
   * Loads the default configuration file name and copies the additionalProperties into it.
   */
  public DownloadWorkflowModule(WorkflowConfiguration workflowConfiguration, DownloadJobConfiguration configuration) {
    this.configuration = Optional.fromNullable(configuration);
    this.workflowConfiguration = workflowConfiguration;
  }

  /**
   * Loads the default configuration file name 'occurrence-download.properties'.
   */
  public DownloadWorkflowModule(WorkflowConfiguration workflowConfiguration) {
    this.workflowConfiguration = workflowConfiguration;
    configuration = Optional.absent();
  }

  @Override
  protected void configure() {
    Names.bindProperties(binder(), workflowConfiguration.getDownloadSettings());
    install(new SolrModule(SolrConfig.fromProperties(workflowConfiguration.getDownloadSettings(),
            PROPERTIES_SOLR_PREFIX)));
    bind(OccurrenceMapReader.class);
    bind(DownloadPrepareAction.class);
    bind(WorkflowConfiguration.class).toInstance(workflowConfiguration);
    if (configuration.isPresent()) {
      bind(DownloadJobConfiguration.class).toInstance(configuration.get());
    }
    bind(RegistryClientUtil.class).toInstance(new RegistryClientUtil(workflowConfiguration.getDownloadSettings()));
    bindDownloadFilesBuilding();
  }

  @Provides
  @Singleton
  CuratorFramework provideCuratorFramework(@Named(PROPERTIES_PREFIX + "zookeeper.namespace") String zookeeperNamespace,
                                           @Named(PROPERTIES_PREFIX + "zookeeper.quorum") String zookeeperConnection,
                                           @Named(PROPERTIES_PREFIX + "zookeeper.sleep_time") Integer sleepTime,
                                           @Named(PROPERTIES_PREFIX + "zookeeper.max_retries") Integer maxRetries) {
    CuratorFramework curator = CuratorFrameworkFactory.builder().namespace(zookeeperNamespace)
      .retryPolicy(new ExponentialBackoffRetry(sleepTime, maxRetries))
      .connectString(zookeeperConnection)
      .build();
    curator.start();
    return curator;
  }

  @Provides
  @Singleton
  DatasetOccurrenceDownloadUsageService provideDatasetOccurrenceDownloadUsageService(
    @Named(DefaultSettings.REGISTRY_URL_KEY) String registryWsUri, RegistryClientUtil registryClientUtil
  ) {
    return registryClientUtil.setupDatasetUsageService(registryWsUri);
  }

  @Provides
  @Singleton
  DatasetService provideDatasetService(@Named(DefaultSettings.REGISTRY_URL_KEY) String registryWsUri,
                                       RegistryClientUtil registryClientUtil) {
    return registryClientUtil.setupDatasetService(registryWsUri);
  }

  @Provides
  @Singleton
  OccurrenceDownloadService provideOccurrenceDownloadService(
    @Named(DefaultSettings.REGISTRY_URL_KEY) String registryWsUri, RegistryClientUtil registryClientUtil) {
    return registryClientUtil.setupOccurrenceDownloadService(registryWsUri);
  }

  @Provides
  ExecutionContextExecutorService provideExecutionContextExecutorService(
    @Named(PROPERTIES_PREFIX + "job.max_threads") int maxThreads
  ) {
    return ExecutionContexts.fromExecutorService(Executors.newFixedThreadPool(maxThreads));
  }

  @Provides
  Connection provideHBaseConnection()
    throws IOException {
    return ConnectionFactory.createConnection(HBaseConfiguration.create());
  }

  @Provides
  LockFactory provideLock(
    CuratorFramework curatorFramework, @Named(PROPERTIES_PREFIX + "max_global_threads") Integer maxGlobalThreads
  ) {
    return new ZooKeeperLockFactory(curatorFramework, maxGlobalThreads, LOCKING_PATH);
  }

  /**
   * Binds a DownloadFilesAggregator according to the DownloadFormat set using the key DOWNLOAD_FORMAT_KEY.
   */
  private void bindDownloadFilesBuilding() {
    DownloadFormat downloadFormat = workflowConfiguration.getDownloadFormat();
    if (downloadFormat != null) {
      if (DownloadFormat.DWCA == downloadFormat) {
        bind(DownloadAggregator.class).to(DwcaDownloadAggregator.class);
      } else if (DownloadFormat.SIMPLE_CSV == downloadFormat) {
        bind(DownloadAggregator.class).to(SimpleCsvDownloadAggregator.class);
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
    public static final String OCC_HBASE_TABLE_KEY = "hbase.table";

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

    public static final String DOWNLOAD_USER_KEY = PROPERTIES_PREFIX + "ws.username";
    public static final String DOWNLOAD_PASSWORD_KEY = PROPERTIES_PREFIX + "ws.password";
    public static final String DOWNLOAD_LINK_KEY = PROPERTIES_PREFIX + "link";
    public static final String HDFS_OUTPUT_PATH_KEY = PROPERTIES_PREFIX + "hdfsOutputPath";
    public static final String TMP_DIR_KEY = PROPERTIES_PREFIX + "tmp.dir";
    public static final String HIVE_DB_PATH_KEY = PROPERTIES_PREFIX + "hive.hdfs.out";

  }

}
